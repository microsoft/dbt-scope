"""OOP Delta Lake access for dbt-scope.

This module centralizes all DuckDB-backed Delta Lake reads so the adapter and
integration tests share one implementation for:

- file-locked Azure token acquisition
- DuckDB extension loading
- Delta table introspection and validation
- ADLS-backed Delta log enumeration
"""

from __future__ import annotations

import re
import time
from abc import ABC, abstractmethod
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from typing import Any

import duckdb
from azure.core.credentials import AccessToken, TokenCredential
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
