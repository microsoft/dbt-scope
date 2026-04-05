"""OOP Delta Lake access for dbt-scope.

This module centralizes all DuckDB-backed Delta Lake reads so the adapter and
integration tests share one implementation for:

- file-locked Azure token acquisition
- DuckDB extension loading
- Delta table introspection and validation
- ADLS-backed Delta log enumeration
"""

from __future__ import annotations

import logging
import re
from abc import ABC, abstractmethod
from collections.abc import Callable, Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from functools import lru_cache
from typing import Any

import duckdb
from azure.core.credentials import AccessToken, TokenCredential
from azure.identity import AzureCliCredential
from azure.storage.filedatalake import DataLakeServiceClient
from dbt_common.exceptions import DbtRuntimeError

from dbt.adapters.scope._file_lock import AZ_CLI_TOKEN_LOCK, FileLock

log = logging.getLogger(__name__)

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


class LockedTokenCredential(TokenCredential):
    """Serialize token acquisition for credentials that share a cache on disk."""

    def __init__(self, credential: TokenCredential, lock_file: str = AZ_CLI_TOKEN_LOCK) -> None:
        self._credential = credential
        self._lock_file = lock_file

    def get_token(self, *scopes: str, claims: str | None = None, **kwargs: Any) -> AccessToken:
        with FileLock(self._lock_file):
            if claims is None:
                return self._credential.get_token(*scopes, **kwargs)
            return self._credential.get_token(*scopes, claims=claims, **kwargs)


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
        except Exception:
            log.info("table_exists(%s) → False (not found or error)", delta_location, exc_info=True)
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
                log.info("get_max_partition(%s, %s) → %s", delta_location, partition_col, result)
                return result
            return None
        except Exception:
            log.info(
                "get_max_partition(%s, %s) → None (error)",
                delta_location,
                partition_col,
                exc_info=True,
            )
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
            log.debug("get_columns(%s) → %s", delta_location, columns)
            return columns
        except Exception:
            log.debug("get_columns(%s) → None (error)", delta_location, exc_info=True)
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
        log.debug("validate_partition_column(%s, %s) → OK", delta_location, partition_col)

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
            log.warning("count_delta_log_files: bad path format: %s", delta_location)
            return 0

        delta_log_prefix = f"{parsed.path.rstrip('/')}/_delta_log/"
        table_paths = self.list_table_paths(delta_location)
        json_count = sum(
            1
            for path in table_paths
            if path.startswith(delta_log_prefix) and path.endswith(".json")
        )
        log.info("count_delta_log_files(%s) → %d JSON files", delta_location, json_count)
        return json_count

    def describe_table_files(self, delta_location: str) -> dict[str, Any]:
        """Summarize parquet files and partition values from ADLS file listing."""
        parsed = AbfssLocation.parse(delta_location)
        if parsed is None:
            log.warning("describe_table_files: bad path format: %s", delta_location)
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
        log.info(
            "describe_table_files(%s) → %d parquet files, %d partitions",
            delta_location,
            len(parquet_paths),
            len(partitions),
        )
        return {"parquet_count": len(parquet_paths), "partitions": partitions}


class DuckDbDeltaLakeClient(DeltaLakeClient):
    """DuckDB-backed implementation of the Delta Lake inspection contract."""

    def __init__(
        self,
        credential: TokenCredential,
        *,
        connection_factory: Callable[[], duckdb.DuckDBPyConnection] | None = None,
        lock_file: str = AZ_CLI_TOKEN_LOCK,
    ) -> None:
        self._credential = LockedTokenCredential(credential, lock_file=lock_file)
        self._connection_factory = connection_factory or duckdb.connect

    @contextmanager
    def connect(self) -> Iterator[duckdb.DuckDBPyConnection]:
        """Yield a DuckDB connection configured for Delta + Azure access."""
        conn = self._connection_factory()
        try:
            conn.execute(_DUCKDB_EXTENSION_SQL)
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

        try:
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
        except Exception:
            log.warning("list_table_paths(%s) failed", delta_location, exc_info=True)
            return []


@lru_cache(maxsize=1)
def get_default_delta_client() -> DuckDbDeltaLakeClient:
    """Return the default Delta client used by the adapter and test helpers."""
    return DuckDbDeltaLakeClient(credential=AzureCliCredential())
