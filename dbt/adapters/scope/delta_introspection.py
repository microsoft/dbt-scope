"""Delta introspection — detect existing Delta tables and query partition metadata.

Uses DuckDB with the delta + azure extensions to read Delta transaction logs
directly from ADLS Gen2.  All functions degrade gracefully on failure (return
False / None) so the adapter falls back to reprocessing everything.
"""

from __future__ import annotations

import logging
import re

import duckdb
from azure.identity import AzureCliCredential

from dbt.adapters.scope._file_lock import AZ_CLI_TOKEN_LOCK, FileLock

log = logging.getLogger(__name__)

_ABFSS_RE = re.compile(
    r"abfss://(?P<container>[^@]+)@(?P<account>[^.]+)\.dfs\.core\.windows\.net/(?P<path>.+)"
)


def _get_storage_token() -> str:
    """Acquire an Azure Storage access token using file-locked AzureCliCredential."""
    cred = AzureCliCredential()
    with FileLock(AZ_CLI_TOKEN_LOCK):
        token = cred.get_token("https://storage.azure.com/.default")
    return token.token


def _make_duckdb_conn() -> duckdb.DuckDBPyConnection:
    """Create a DuckDB connection with delta + azure extensions loaded."""
    conn = duckdb.connect()
    conn.execute("INSTALL delta; LOAD delta; INSTALL azure; LOAD azure;")
    token = _get_storage_token()
    conn.execute(f"CREATE SECRET az1 (TYPE AZURE, PROVIDER ACCESS_TOKEN, ACCESS_TOKEN '{token}');")
    return conn


def delta_table_exists(delta_location: str) -> bool:
    """Check whether a Delta table exists and is readable at *delta_location*.

    Returns ``True`` if DuckDB can open the Delta log, ``False`` on any error
    (missing table, bad path, network issues, etc.).
    """
    conn = None
    try:
        conn = _make_duckdb_conn()
        conn.execute(f"SELECT 1 FROM delta_scan('{delta_location}') LIMIT 0")
        return True
    except Exception:
        log.info("delta_table_exists(%s) → False (not found or error)", delta_location)
        return False
    finally:
        if conn is not None:
            conn.close()


def get_max_partition(delta_location: str, partition_col: str) -> str | None:
    """Query ``MAX(partition_col)`` from a Delta table via DuckDB.

    Returns the max value as a string (e.g. ``"20260404"``), or ``None`` if the
    table is empty or unreadable.
    """
    conn = None
    try:
        conn = _make_duckdb_conn()
        row = conn.execute(
            f"SELECT MAX({partition_col}) AS mv FROM delta_scan('{delta_location}')"
        ).fetchone()
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
    finally:
        if conn is not None:
            conn.close()


def get_delta_columns(delta_location: str) -> list[str] | None:
    """Return the column names of a Delta table, or ``None`` if unreadable.

    Uses DuckDB ``delta_scan`` with ``LIMIT 0`` to read the schema without
    scanning data.
    """
    conn = None
    try:
        conn = _make_duckdb_conn()
        description = conn.execute(
            f"SELECT * FROM delta_scan('{delta_location}') LIMIT 0"
        ).description
        columns = [col[0] for col in description]
        log.debug("get_delta_columns(%s) → %s", delta_location, columns)
        return columns
    except Exception:
        log.debug("get_delta_columns(%s) → None (error)", delta_location, exc_info=True)
        return None
    finally:
        if conn is not None:
            conn.close()


def validate_partition_column(delta_location: str, partition_col: str) -> None:
    """Raise if the Delta table at *delta_location* exists but lacks *partition_col*.

    This enforces that incremental models have the expected column present.
    If the table doesn't exist or can't be read, the check is silently skipped
    (the table will be created with the correct schema on first run).
    """
    columns = get_delta_columns(delta_location)
    if columns is None:
        return  # Table doesn't exist or DuckDB can't read it — skip validation
    if partition_col not in columns:
        from dbt_common.exceptions import DbtRuntimeError

        raise DbtRuntimeError(
            f"Delta table at '{delta_location}' exists but does not contain "
            f"column '{partition_col}'. "
            f"Available columns: {columns}. "
            f"The model requires column '{partition_col}' for incremental processing."
        )
    log.debug(
        "validate_partition_column(%s, %s) → OK",
        delta_location,
        partition_col,
    )


def parse_abfss(location: str) -> tuple[str, str, str] | None:
    """Parse an ``abfss://`` URL into ``(container, account, path)``.

    Returns ``None`` if the URL doesn't match the expected format.
    """
    m = _ABFSS_RE.match(location)
    if not m:
        return None
    return m.group("container"), m.group("account"), m.group("path")
