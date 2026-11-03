"""OOP Delta Lake access for dbt-scope.

This module centralizes all DuckDB-backed Delta Lake reads so the adapter and
integration tests share one implementation for:

- file-locked Azure token acquisition
- DuckDB extension loading
- Delta table introspection and validation
- ADLS-backed Delta log enumeration
"""

from __future__ import annotations

import json
import re
import time
from abc import ABC, abstractmethod
from collections.abc import Callable, Iterable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Any
from urllib.parse import unquote

import duckdb
from azure.core.credentials import AccessToken, TokenCredential
from azure.core.exceptions import ResourceNotFoundError
from azure.identity import AzureCliCredential, CredentialUnavailableError
from azure.storage.filedatalake import DataLakeServiceClient
from dbt.adapters.events.logging import AdapterLogger
from dbt_common.exceptions import DbtRuntimeError

from dbt.adapters.scope._file_lock import AZ_CLI_TOKEN_LOCK, FABRIC_TOKEN_LOCK, FileLock
from dbt.adapters.scope.message_retry import MessageRetryPolicy, retry_on_message

log = AdapterLogger("scope")

_ABFSS_RE = re.compile(
    r"abfss://(?P<container>[^@]+)@(?P<account>[^.]+)\.dfs\.core\.windows\.net/(?P<path>.+)"
)
_STORAGE_SCOPE = "https://storage.azure.com/.default"
_DUCKDB_EXTENSION_SQL = "INSTALL delta; LOAD delta; INSTALL azure; LOAD azure;"
_COMMIT_FILE_RE = re.compile(r"(?:^|/)(\d{20})\.json$")
_CHECKPOINT_FILE_RE = re.compile(r"(?:^|/)(\d{20})\.checkpoint(?:\.\d+\.\d+)?\.parquet$")
_PHYSICAL_NAME_KEY = "delta.columnMapping.physicalName"


def parse_abfss(location: str) -> tuple[str, str, str] | None:
    """Parse an ``abfss://`` URL into ``(container, account, path)``."""
    match = _ABFSS_RE.match(location)
    if not match:
        return None
    return match.group("container"), match.group("account"), match.group("path")


def _sql_literal(value: str) -> str:
    """Escape a string for safe embedding in a SQL literal."""
    return value.replace("'", "''")


def _sql_identifier(value: str) -> str:
    """Escape a string for safe embedding as a quoted SQL identifier."""
    escaped = value.replace('"', '""')
    return f'"{escaped}"'


# ---------------------------------------------------------------------------
# Delta Lake schema evolution
# ---------------------------------------------------------------------------

# Canonical equivalence between SCOPE type spellings (as written in a model's
# ``delta_table_columns``) and the DuckDB type names surfaced by ``delta_scan``.
# Both spellings map to a single token so a dbt ``long`` and a DuckDB ``BIGINT``
# compare equal. SCOPE and DuckDB spellings do not collide, so one table works
# for both sides.
_CANONICAL_TYPES: dict[str, str] = {
    # strings
    "string": "STRING",
    "varchar": "STRING",
    "char": "STRING",
    "text": "STRING",
    # booleans
    "bool": "BOOL",
    "boolean": "BOOL",
    # integers (width matters: SCOPE int == 32-bit, long == 64-bit)
    "sbyte": "INT8",
    "tinyint": "INT8",
    "int8": "INT8",
    "short": "INT16",
    "smallint": "INT16",
    "int16": "INT16",
    "int": "INT32",
    "integer": "INT32",
    "int32": "INT32",
    "long": "INT64",
    "bigint": "INT64",
    "int64": "INT64",
    # floating point
    "float": "FLOAT32",
    "real": "FLOAT32",
    "float4": "FLOAT32",
    "double": "FLOAT64",
    "float8": "FLOAT64",
    # temporal
    "datetime": "DATETIME",
    "date": "DATE",
    # binary
    "byte[]": "BINARY",
    "binary": "BINARY",
    "varbinary": "BINARY",
    "blob": "BINARY",
    "bytea": "BINARY",
}


def canonical_type(raw: str | None) -> str | None:
    """Normalize a SCOPE or DuckDB type name to a canonical token.

    Returns ``None`` for unknown types so callers can stay lenient (a type we
    cannot confidently normalize must not trigger a spurious mismatch failure).
    """
    if not raw:
        return None
    base = raw.strip().rstrip("?").strip().lower()
    if base.startswith("timestamp"):
        return "DATETIME"
    if base.startswith(("decimal", "numeric")):
        return "DECIMAL"
    paren = base.find("(")
    if paren != -1:
        base = base[:paren].strip()
    return _CANONICAL_TYPES.get(base)


def _format_schema(columns: list[tuple[str, str]]) -> str:
    """Render ``[(name, type), ...]`` as an indented, aligned block."""
    if not columns:
        return "    (empty)"
    width = max(len(name) for name, _ in columns)
    return "\n".join(f"    {name.ljust(width)}  {dtype}" for name, dtype in columns)


def diff_schema_for_evolution(
    dbt_columns: list[dict[str, str]],
    existing_schema: dict[str, str],
    *,
    partition_columns: tuple[str, ...] = (),
    location: str = "",
) -> list[dict[str, str]]:
    """Diff a model's declared columns against the live Delta table schema.

    Args:
        dbt_columns: ``[{"name": .., "type": ..}, ...]`` from ``delta_table_columns``
            (``type`` is a SCOPE type spelling).
        existing_schema: ``{column_name: duckdb_type}`` read from the Delta table.
        partition_columns: partition column names — excluded from the diff because
            they are declared in ``CREATE TABLE`` and cannot be ``ALTER ADD COLUMN``-ed.
        location: Delta table location, for error messages only.

    Returns:
        The subset of ``dbt_columns`` that are absent from the Delta table and must
        be added via ``ALTER TABLE ... ADD COLUMN``.

    Raises:
        DbtRuntimeError: when a column exists in the Delta table but not in the model
            (dbt-scope never drops columns), or when a column's type changed.
    """
    ignore = {c.lower() for c in partition_columns}
    existing_by_lower = {name.lower(): (name, dtype) for name, dtype in existing_schema.items()}
    dbt_by_lower = {c["name"].lower(): c for c in dbt_columns}

    to_add: list[dict[str, str]] = []
    type_mismatches: list[str] = []

    for lower, col in dbt_by_lower.items():
        if lower in ignore:
            continue
        if lower not in existing_by_lower:
            to_add.append(col)
            continue
        _, existing_type = existing_by_lower[lower]
        dbt_canon = canonical_type(col["type"])
        existing_canon = canonical_type(existing_type)
        # Only flag a mismatch when both sides are confidently known and differ.
        if dbt_canon is not None and existing_canon is not None and dbt_canon != existing_canon:
            type_mismatches.append(
                f"    {col['name']}: model declares '{col['type']}' "
                f"but the Delta table has '{existing_type}'"
            )

    missing_in_dbt = [
        orig
        for lower, (orig, _dtype) in existing_by_lower.items()
        if lower not in dbt_by_lower and lower not in ignore
    ]

    if missing_in_dbt or type_mismatches:
        dbt_block = _format_schema([(c["name"], c["type"]) for c in dbt_columns])
        delta_block = _format_schema(sorted(existing_schema.items()))
        problems: list[str] = []
        if missing_in_dbt:
            problems.append(
                "Columns present in the Delta table but MISSING from the model "
                "(dbt-scope does not drop columns):\n    " + ", ".join(sorted(missing_in_dbt))
            )
        if type_mismatches:
            problems.append(
                "Columns whose type changed (dbt-scope cannot evolve column types):\n"
                + "\n".join(type_mismatches)
            )
        raise DbtRuntimeError(
            f"Delta table schema at '{location}' is incompatible with the dbt model.\n\n"
            f"Model delta_table_columns:\n{dbt_block}\n\n"
            f"Existing Delta table schema:\n{delta_block}\n\n"
            + "\n\n".join(problems)
            + "\n\nFix your dbt model's delta_table_columns to match the Delta table "
            "(re-add the missing columns and/or revert the changed types), or migrate "
            "the table out of band. Only ADDING new columns is supported automatically."
        )

    return to_add


@dataclass(frozen=True)
class AbfssLocation:
    """Structured representation of an ``abfss://`` path."""

    container: str
    account: str
    path: str

    @classmethod
    def parse(cls, location: str) -> AbfssLocation | None:
        parsed = parse_abfss(location)
        if parsed is None:
            return None
        return cls(*parsed)

    @property
    def account_url(self) -> str:
        return f"https://{self.account}.dfs.core.windows.net"

    @property
    def url(self) -> str:
        host = f"{self.container}@{self.account}.dfs.core.windows.net"
        return f"abfss://{host}/{self.path.strip('/')}"


@dataclass(frozen=True)
class DeltaCommit:
    """One parsed ``_delta_log/<version>.json`` commit."""

    version: int
    actions: list[dict[str, Any]] = field(default_factory=list)
    last_modified_ms: int | None = None

    @classmethod
    def parse(cls, version: int, text: str, last_modified_ms: int | None = None) -> DeltaCommit:
        actions: list[dict[str, Any]] = []
        for line in text.splitlines():
            if not line.strip():
                continue
            try:
                action = json.loads(line)
            except ValueError:
                log.debug(f"Skipping unparseable action in Delta commit {version}")
                continue
            if isinstance(action, dict):
                actions.append(action)
        return cls(version=version, actions=actions, last_modified_ms=last_modified_ms)

    def _first(self, key: str) -> dict[str, Any] | None:
        for action in self.actions:
            value = action.get(key)
            if isinstance(value, dict):
                return value
        return None

    @property
    def commit_info(self) -> dict[str, Any] | None:
        return self._first("commitInfo")

    @property
    def metadata(self) -> dict[str, Any] | None:
        return self._first("metaData")

    @property
    def configuration(self) -> dict[str, Any]:
        configuration = (self.metadata or {}).get("configuration")
        return configuration if isinstance(configuration, dict) else {}

    @property
    def schema_string(self) -> str | None:
        schema = (self.metadata or {}).get("schemaString")
        return schema if isinstance(schema, str) else None

    @property
    def is_scope_engine(self) -> bool:
        """ScopeEngine writes ``engineInfo: ScopeEngine/...``; its table-creating commit has none."""
        info = self.commit_info
        if info is None:
            return True
        return str(info.get("engineInfo") or "").startswith("ScopeEngine")

    @property
    def added_data_paths(self) -> list[str]:
        return [
            add["path"]
            for action in self.actions
            if isinstance(add := action.get("add"), dict)
            and add.get("path")
            and add.get("dataChange", True)
        ]

    @property
    def timestamp_ms(self) -> int | None:
        info = self.commit_info or {}
        if isinstance(info.get("timestamp"), int):
            return info["timestamp"]
        add_times = [
            add["modificationTime"]
            for action in self.actions
            if isinstance(add := action.get("add"), dict)
            and isinstance(add.get("modificationTime"), int)
        ]
        if add_times:
            return max(add_times)
        return self.last_modified_ms

    def carries_property(self, key: str, value: str) -> bool:
        """Return ``True`` if this commit set table property ``key`` to ``value``."""
        if self.configuration.get(key) == value:
            return True
        parameters = (self.commit_info or {}).get("operationParameters")
        if not parameters:
            return False
        serialized = parameters if isinstance(parameters, str) else json.dumps(parameters)
        return key in serialized and value in serialized


def physical_column_name(schema_string: str | None, column: str) -> str | None:
    """Return the parquet column name of logical ``column``, or ``None`` if it is absent.

    Honours Delta column mapping (``delta.columnMapping.physicalName``).
    """
    if not schema_string:
        return None
    try:
        fields = json.loads(schema_string).get("fields", [])
    except (ValueError, AttributeError):
        return None
    matches = [f for f in fields if isinstance(f, dict) and f.get("name") == column]
    if not matches:
        matches = [
            f
            for f in fields
            if isinstance(f, dict) and str(f.get("name", "")).lower() == column.lower()
        ]
    if not matches:
        return None
    metadata = matches[0].get("metadata") or {}
    return metadata.get(_PHYSICAL_NAME_KEY) or matches[0]["name"]


@dataclass(frozen=True)
class RetryPolicy:
    """Linear-backoff retry policy for transient credential failures.

    ``max_retries`` is the number of additional attempts AFTER the first
    try — matching the semantics of urllib3's ``Retry(total=...)``.
    Total attempts == ``max_retries + 1``.

    Delay between attempts is ``min(attempt * initial_delay_seconds,
    max_delay_seconds)`` (linear, capped). No jitter — keep it
    deterministic for testing.
    """

    max_retries: int = 10
    initial_delay_seconds: float = 1.0
    max_delay_seconds: float = 10.0

    @classmethod
    def from_http_retries(cls, http_retries: int | None) -> RetryPolicy:
        """Build a policy from the ``http_retries`` profile field.

        Reuses the same field as the urllib3 HTTP retry count for
        consistency. ``None`` (or any value below 0) returns the
        defaults: 10 retries, 1s linear, 10s cap.
        """
        if http_retries is None or http_retries < 0:
            return cls()
        return cls(
            max_retries=http_retries,
            initial_delay_seconds=1.0,
            max_delay_seconds=10.0,
        )


class LockedTokenCredential(TokenCredential):
    """Serialize token acquisition for credentials that share a cache on disk."""

    def __init__(
        self,
        credential: TokenCredential,
        lock_file: str = AZ_CLI_TOKEN_LOCK,
        retry_policy: RetryPolicy | None = None,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self._credential = credential
        self._lock_file = lock_file
        self._retry_policy = retry_policy or RetryPolicy()
        self._sleep = sleep

    def get_token(self, *scopes: str, claims: str | None = None, **kwargs: Any) -> AccessToken:
        # ``CredentialUnavailableError`` is what ``AzureCliCredential`` raises
        # when the underlying ``az`` subprocess times out or otherwise fails
        # transiently (it wraps ``subprocess.TimeoutExpired`` and friends).
        # Retry with linear backoff while releasing the file lock between
        # attempts so other workers get a fair chance at the lock.
        policy = self._retry_policy
        last_exc: CredentialUnavailableError | None = None
        for attempt in range(1, policy.max_retries + 2):  # +1 for the initial try
            try:
                with FileLock(self._lock_file):
                    if claims is None:
                        return self._credential.get_token(*scopes, **kwargs)
                    return self._credential.get_token(*scopes, claims=claims, **kwargs)
            except CredentialUnavailableError as exc:
                last_exc = exc
                if attempt > policy.max_retries:
                    log.error(
                        f"Azure credential acquisition failed after "
                        f"{policy.max_retries + 1} attempts: {exc.message}"
                    )
                    raise
                delay = min(policy.initial_delay_seconds * attempt, policy.max_delay_seconds)
                log.warning(
                    f"Azure credential acquisition failed "
                    f"(attempt {attempt}/{policy.max_retries + 1}): "
                    f"{exc.message}. Retrying in {delay:.1f}s"
                )
                self._sleep(delay)
        # Unreachable: the loop either returns or raises. Keep mypy happy.
        assert last_exc is not None
        raise last_exc


def build_credential(
    credentials: Any, *, retry_policy: RetryPolicy | None = None
) -> TokenCredential:
    """Return the configured TokenCredential for a ScopeCredentials object,
    always wrapped in ``LockedTokenCredential``.

    The file lock serializes concurrent dbt threads through a single token
    acquisition. Without it, 4 parallel workers each independently walk the
    inner credential's fallback chain — which on headless Fabric notebooks can
    land on interactive device-code auth (one prompt per thread).

    - ``authentication='cli'``: wraps ``AzureCliCredential()``. File lock and
      transient-error retry are tuned for the ``az`` subprocess token cache.
    - ``authentication='token_credential'``: wraps the user-supplied credential
      (e.g. ``EntraTokenCredential``). The first thread populates the cache;
      subsequent threads reuse the cached token without re-entering the inner
      credential's fallback chain.
    """
    policy = retry_policy or RetryPolicy.from_http_retries(
        getattr(credentials, "http_retries", None)
    )
    auth = (getattr(credentials, "authentication", "cli") or "cli").lower()
    if auth == "token_credential":
        # Lazy import keeps `delta_lake.py` importable in places that don't
        # need the custom-credential plumbing.
        from dbt.adapters.scope.custom_credential import load_custom_credential

        inner: TokenCredential = load_custom_credential(
            credentials.credential_class, credentials.credential_kwargs
        )
        lock_file = FABRIC_TOKEN_LOCK
    else:
        inner = AzureCliCredential()
        lock_file = AZ_CLI_TOKEN_LOCK
    return LockedTokenCredential(
        inner,
        lock_file=lock_file,
        retry_policy=policy,
    )


class DeltaLakeClient(ABC):
    """Abstract read-only contract for Delta Lake inspection and verification."""

    @contextmanager
    @abstractmethod
    def connect(self) -> Iterator[duckdb.DuckDBPyConnection]:
        """Yield a configured query connection that can read Delta tables."""

    @abstractmethod
    def list_table_paths(self, delta_location: str) -> list[str]:
        """Return all file paths stored under a Delta table root."""

    def fetchone(self, query: str) -> tuple[Any, ...] | None:
        """Execute a read-only query and return the first row, if any."""
        with self.connect() as conn:
            return conn.execute(query).fetchone()

    def fetchall(self, query: str) -> list[tuple[Any, ...]]:
        """Execute a read-only query and return all rows."""
        with self.connect() as conn:
            return conn.execute(query).fetchall()

    def table_exists(self, delta_location: str) -> bool:
        """Return ``True`` if DuckDB can open the Delta table."""
        try:
            escaped_location = _sql_literal(delta_location)
            self.fetchone(f"SELECT 1 FROM delta_scan('{escaped_location}') LIMIT 0")
            return True
        except CredentialUnavailableError:
            log.error(f"table_exists: credential acquisition exhausted for {delta_location}")
            raise
        except Exception:
            log.debug(f"table_exists({delta_location}) → False (not found or error)")
            return False

    def get_max_partition(self, delta_location: str, partition_col: str) -> str | None:
        """Return ``MAX(partition_col)`` as a string, or ``None`` if unreadable."""
        try:
            escaped_location = _sql_literal(delta_location)
            safe_partition_col = _sql_identifier(partition_col)
            row = self.fetchone(
                f"SELECT MAX({safe_partition_col}) AS mv FROM delta_scan('{escaped_location}')"
            )
            if row and row[0] is not None:
                result = str(row[0])
                log.debug(f"get_max_partition({delta_location}, {partition_col}) → {result}")
                return result
            return None
        except CredentialUnavailableError:
            log.error(f"get_max_partition: credential acquisition exhausted for {delta_location}")
            raise
        except Exception:
            log.debug(f"get_max_partition({delta_location}, {partition_col}) → None (error)")
            return None

    def get_columns(self, delta_location: str) -> list[str] | None:
        """Return Delta column names, or ``None`` if the table is unreadable."""
        try:
            escaped_location = _sql_literal(delta_location)
            with self.connect() as conn:
                column_description = conn.execute(
                    f"SELECT * FROM delta_scan('{escaped_location}') LIMIT 0"
                ).description
            columns = [column[0] for column in column_description]
            log.debug(f"get_columns({delta_location}) → {columns!s}")
            return columns
        except CredentialUnavailableError:
            log.error(f"get_columns: credential acquisition exhausted for {delta_location}")
            raise
        except Exception:
            log.debug(f"get_columns({delta_location}) → None (error)")
            return None

    def get_schema(self, delta_location: str) -> dict[str, str] | None:
        """Return the Delta schema as ``{column_name: duckdb_type}``.

        Returns ``None`` when the table is unreadable (does not exist, or the
        Delta log cannot be parsed). Column order is preserved.
        """
        try:
            escaped_location = _sql_literal(delta_location)
            with self.connect() as conn:
                column_description = conn.execute(
                    f"SELECT * FROM delta_scan('{escaped_location}') LIMIT 0"
                ).description
            schema = {column[0]: str(column[1]) for column in column_description}
            log.debug(f"get_schema({delta_location}) → {schema!s}")
            return schema
        except CredentialUnavailableError:
            log.error(f"get_schema: credential acquisition exhausted for {delta_location}")
            raise
        except Exception:
            log.debug(f"get_schema({delta_location}) → None (error)")
            return None

    def validate_partition_column(self, delta_location: str, partition_col: str) -> None:
        """Raise when an existing Delta table lacks the expected partition column."""
        columns = self.get_columns(delta_location)
        if columns is None:
            return
        if partition_col not in columns:
            raise DbtRuntimeError(
                f"Delta table at '{delta_location}' exists but does not contain "
                f"column '{partition_col}'. "
                f"Available columns: {columns}. "
                f"The model requires column '{partition_col}' for incremental processing."
            )
        log.debug(f"validate_partition_column({delta_location}, {partition_col}) → OK")

    def get_total_row_count(self, delta_location: str) -> int:
        """Return the number of rows currently visible in the Delta table."""
        escaped_location = _sql_literal(delta_location)
        row = self.fetchone(f"SELECT COUNT(*) AS cnt FROM delta_scan('{escaped_location}')")
        return int(row[0]) if row else 0

    def get_partition_counts(self, delta_location: str, partition_col: str) -> dict[str, int]:
        """Return a ``partition_value -> row_count`` mapping for a Delta table."""
        escaped_location = _sql_literal(delta_location)
        safe_partition_col = _sql_identifier(partition_col)
        rows = self.fetchall(
            f"SELECT {safe_partition_col}, COUNT(*) AS cnt "
            f"FROM delta_scan('{escaped_location}') "
            f"GROUP BY {safe_partition_col} ORDER BY {safe_partition_col}"
        )
        return {str(row[0]): int(row[1]) for row in rows}

    def count_delta_log_files(self, delta_location: str) -> int:
        """Count JSON transaction log files under ``_delta_log/``."""
        parsed = AbfssLocation.parse(delta_location)
        if parsed is None:
            log.warning(f"count_delta_log_files: bad path format: {delta_location}")
            return 0

        delta_log_prefix = f"{parsed.path.rstrip('/')}/_delta_log/"
        table_paths = self.list_table_paths(delta_location)
        json_count = sum(
            1
            for path in table_paths
            if path.startswith(delta_log_prefix) and path.endswith(".json")
        )
        log.debug(f"count_delta_log_files({delta_location}) → {json_count} JSON files")
        return json_count

    def describe_table_files(self, delta_location: str) -> dict[str, Any]:
        """Summarize parquet files and partition values from ADLS file listing."""
        parsed = AbfssLocation.parse(delta_location)
        if parsed is None:
            log.warning(f"describe_table_files: bad path format: {delta_location}")
            return {"parquet_count": 0, "partitions": [], "error": f"Bad path: {delta_location}"}

        table_paths = self.list_table_paths(delta_location)
        parquet_paths = [path for path in table_paths if path.endswith(".parquet")]
        partitions = sorted(
            {
                match.group(1)
                for path in table_paths
                if (match := re.search(r"event_year_date[^=]*=(\d+)", path))
            }
        )
        log.debug(
            f"describe_table_files({delta_location}) → "
            f"{len(parquet_paths)} parquet files, {len(partitions)} partitions"
        )
        return {"parquet_count": len(parquet_paths), "partitions": partitions}


class DuckDbDeltaLakeClient(DeltaLakeClient):
    """DuckDB-backed implementation of the Delta Lake inspection contract."""

    def __init__(
        self,
        credential: TokenCredential,
        *,
        connection_factory: Callable[[], duckdb.DuckDBPyConnection] | None = None,
        message_retry_policy: MessageRetryPolicy | None = None,
    ) -> None:
        self._credential = credential
        self._connection_factory = connection_factory or duckdb.connect
        self._message_retry_policy = message_retry_policy or MessageRetryPolicy.disabled()

    @contextmanager
    def connect(self) -> Iterator[duckdb.DuckDBPyConnection]:
        """Yield a DuckDB connection configured for Delta + Azure access."""
        conn = self._connection_factory()
        try:
            conn.execute(_DUCKDB_EXTENSION_SQL)
            conn.execute("SET azure_transport_option_type = 'curl';")
            token = self._credential.get_token(_STORAGE_SCOPE).token
            escaped_token = _sql_literal(token)
            conn.execute(
                f"CREATE SECRET az1 (TYPE AZURE, PROVIDER ACCESS_TOKEN, "
                f"ACCESS_TOKEN '{escaped_token}');"
            )
            yield conn
        finally:
            conn.close()

    def list_table_paths(self, delta_location: str) -> list[str]:
        """Enumerate the file paths that exist beneath a Delta table root."""
        parsed = AbfssLocation.parse(delta_location)
        if parsed is None:
            return []

        def _list() -> list[str]:
            service = DataLakeServiceClient(
                account_url=parsed.account_url,
                credential=self._credential,
            )
            file_system = service.get_file_system_client(parsed.container)
            prefix = parsed.path.rstrip("/")
            return [
                path.name
                for path in file_system.get_paths(path=prefix, recursive=True)
                if not getattr(path, "is_directory", False)
            ]

        try:
            return retry_on_message(
                _list,
                policy=self._message_retry_policy,
                label=f"delta_lake.list_table_paths {delta_location}",
            )
        except CredentialUnavailableError:
            log.error(f"list_table_paths: credential acquisition exhausted for {delta_location}")
            raise
        except Exception:
            log.warning(f"list_table_paths({delta_location}) failed")
            return []

    def delta_log_exists(self, delta_location: str) -> bool:
        """Return ``True`` if a ``_delta_log/*.json`` commit exists for the table.

        Lists only the ``_delta_log/`` prefix (not the whole table) and exits on
        the first JSON commit file, so it stays cheap on large tables.
        """
        parsed = AbfssLocation.parse(delta_location)
        if parsed is None:
            return False

        def _exists() -> bool:
            service = DataLakeServiceClient(
                account_url=parsed.account_url,
                credential=self._credential,
            )
            file_system = service.get_file_system_client(parsed.container)
            prefix = f"{parsed.path.rstrip('/')}/_delta_log"
            for path in file_system.get_paths(path=prefix, recursive=True):
                if not getattr(path, "is_directory", False) and path.name.endswith(".json"):
                    return True
            return False

        try:
            result = retry_on_message(
                _exists,
                policy=self._message_retry_policy,
                label=f"delta_lake.delta_log_exists {delta_location}",
            )
            log.debug(f"delta_log_exists({delta_location}) → {result}")
            return result
        except CredentialUnavailableError:
            log.error(f"delta_log_exists: credential acquisition exhausted for {delta_location}")
            raise
        except Exception:
            # A missing path raises (path not found) — treat as "does not exist".
            log.debug(f"delta_log_exists({delta_location}) → False (not found or error)")
            return False

    def _delta_log_prefix(self, delta_location: str) -> tuple[AbfssLocation, str]:
        parsed = AbfssLocation.parse(delta_location)
        if parsed is None:
            raise DbtRuntimeError(f"Invalid Delta location: {delta_location}")
        return parsed, f"{parsed.path.rstrip('/')}/_delta_log"

    def _file_system(self, parsed: AbfssLocation) -> Any:
        service = DataLakeServiceClient(account_url=parsed.account_url, credential=self._credential)
        return service.get_file_system_client(parsed.container)

    def _list_log(self, delta_location: str) -> tuple[list[int], dict[int, list[str]]]:
        """Return the JSON commit versions in ``_delta_log/`` (ascending) and checkpoint files by version."""
        parsed, prefix = self._delta_log_prefix(delta_location)

        def _list() -> tuple[list[int], dict[int, list[str]]]:
            try:
                paths = list(self._file_system(parsed).get_paths(path=prefix, recursive=False))
            except ResourceNotFoundError:
                return [], {}
            commits: list[int] = []
            checkpoints: dict[int, list[str]] = {}
            for path in paths:
                if getattr(path, "is_directory", False):
                    continue
                if match := _COMMIT_FILE_RE.search(path.name):
                    commits.append(int(match.group(1)))
                elif match := _CHECKPOINT_FILE_RE.search(path.name):
                    checkpoints.setdefault(int(match.group(1)), []).append(path.name)
            commits.sort()
            return commits, checkpoints

        return retry_on_message(
            _list,
            policy=self._message_retry_policy,
            label=f"delta_lake.list_log {delta_location}",
        )

    def log_versions(self, delta_location: str) -> tuple[list[int], int]:
        """Return the JSON commit versions in ``_delta_log/`` (ascending) and the latest version.

        The latest version is ``-1`` when the table has no log yet.
        """
        commits, checkpoints = self._list_log(delta_location)
        return commits, max([-1, *commits, *checkpoints])

    def latest_version(self, delta_location: str) -> int:
        """Return the latest Delta table version, or ``-1`` if the table has no log yet."""
        return self.log_versions(delta_location)[1]

    def read_commit(self, delta_location: str, version: int) -> DeltaCommit | None:
        """Read and parse one JSON commit; ``None`` if it does not exist."""
        parsed, prefix = self._delta_log_prefix(delta_location)

        def _read() -> DeltaCommit | None:
            file_client = self._file_system(parsed).get_file_client(f"{prefix}/{version:020d}.json")
            try:
                downloader = file_client.download_file()
                raw = downloader.readall()
            except ResourceNotFoundError:
                return None
            last_modified = getattr(getattr(downloader, "properties", None), "last_modified", None)
            last_modified_ms = int(last_modified.timestamp() * 1000) if last_modified else None
            return DeltaCommit.parse(version, raw.decode("utf-8"), last_modified_ms)

        return retry_on_message(
            _read,
            policy=self._message_retry_policy,
            label=f"delta_lake.read_commit {delta_location}@{version}",
        )

    def read_commits(self, delta_location: str, versions: Iterable[int]) -> list[DeltaCommit]:
        """Read the given JSON commits in ascending order, skipping ones that no longer exist."""
        commits = []
        for version in sorted(versions):
            commit = self.read_commit(delta_location, version)
            if commit is not None:
                commits.append(commit)
        return commits

    def latest_metadata_commit(
        self, delta_location: str, max_commits: int = 100
    ) -> DeltaCommit | None:
        """Return the table's newest ``metaData`` action as a commit.

        Reads the JSON commits after the latest checkpoint, then the checkpoint itself.
        """
        parsed, _ = self._delta_log_prefix(delta_location)
        commits, checkpoints = self._list_log(delta_location)
        checkpoint_version = max(checkpoints, default=-1)
        for version in reversed(commits[-max_commits:]):
            if version <= checkpoint_version:
                break
            commit = self.read_commit(delta_location, version)
            if commit is not None and commit.metadata is not None:
                return commit
        if checkpoint_version < 0:
            return None
        metadata = self._checkpoint_metadata(parsed, checkpoints[checkpoint_version])
        if metadata is None:
            return None
        return DeltaCommit(version=checkpoint_version, actions=[{"metaData": metadata}])

    def _checkpoint_metadata(
        self, parsed: AbfssLocation, checkpoint_files: list[str]
    ) -> dict[str, Any] | None:
        urls = [f"{parsed.url}/_delta_log/{name.rsplit('/', 1)[-1]}" for name in checkpoint_files]
        files_sql = ", ".join(f"'{_sql_literal(url)}'" for url in urls)
        row = self.fetchone(
            "SELECT metaData.schemaString, map_entries(metaData.configuration) "
            f"FROM read_parquet([{files_sql}], union_by_name = true) "
            "WHERE metaData IS NOT NULL LIMIT 1"
        )
        if row is None:
            return None
        schema_string, entries = row
        return {
            "schemaString": schema_string,
            "configuration": {entry["key"]: entry["value"] for entry in entries or []},
        }

    def distinct_column_values(
        self, delta_location: str, data_paths: list[str], column: str
    ) -> set[str]:
        """Return the distinct non-null values of parquet ``column`` across ``data_paths``.

        ``data_paths`` are Delta ``add.path`` values: URL-encoded and relative to the
        table root, or absolute URLs.
        """
        if not data_paths:
            return set()
        parsed, _ = self._delta_log_prefix(delta_location)
        urls = [path if "://" in path else f"{parsed.url}/{unquote(path)}" for path in data_paths]
        files_sql = ", ".join(f"'{_sql_literal(url)}'" for url in urls)
        rows = self.fetchall(
            f"SELECT DISTINCT {_sql_identifier(column)} "
            f"FROM read_parquet([{files_sql}], union_by_name = true)"
        )
        return {str(row[0]) for row in rows if row[0] is not None}
