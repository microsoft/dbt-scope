"""ScopeAdapter — dbt adapter for ADLA SCOPE with Delta table support."""

from __future__ import annotations

import atexit
import signal
import threading
import time
from datetime import datetime, timezone
from typing import Any

import agate
import pandas as pd
import tabulate as tabulate_lib
from dbt.adapters.base import BaseAdapter, available
from dbt.adapters.events.logging import AdapterLogger
from dbt_common.exceptions import DbtRuntimeError

from dbt.adapters.scope.adls_gen1_client import AdlsGen1Client, FileInfo
from dbt.adapters.scope.checkpoint import CheckpointManager, Watermark
from dbt.adapters.scope.column import ScopeColumn
from dbt.adapters.scope.connections import (
    ScopeConnectionHandle,
    ScopeConnectionManager,
    _shutdown_event,
    cancel_all_active_jobs,
)
from dbt.adapters.scope.constants import (
    DEFAULT_MAX_BYTES_PER_TRIGGER,
    DEFAULT_PROCESSING_TIME_TIMEOUT_SECONDS,
    DEFAULT_SAFETY_BUFFER_SECONDS,
    DEFAULT_SOURCE_COMPACTION_INTERVAL,
    DEFAULT_SOURCE_RETENTION_FILES,
    DEFAULT_WAIT_ON_CANCEL_SECONDS,
)
from dbt.adapters.scope.credentials import ScopeCredentials
from dbt.adapters.scope.delta_lake import (
    DuckDbDeltaLakeClient,
    RetryPolicy,
    build_credential,
    diff_schema_for_evolution,
)
from dbt.adapters.scope.file_tracker import FileTracker
from dbt.adapters.scope.message_retry import MessageRetryPolicy, retry_on_message
from dbt.adapters.scope.relation import ScopeRelation
from dbt.adapters.scope.script_builder import ColumnDef, ScriptConfig
from dbt.adapters.scope.trigger_config import parse_trigger_config

log = AdapterLogger("scope")

# ---------------------------------------------------------------------------
# Graceful shutdown support
# ---------------------------------------------------------------------------
_signal_handlers_installed = False
_signal_lock = threading.Lock()
_atexit_registered = False

# Credentials observed across all ScopeConnectionManager.open() calls in this
# process. Used by the signal handler to decide (a) whether to cancel
# in-flight jobs, and (b) how long to wait for ADLA to confirm terminal state.
_observed_credentials: list[ScopeCredentials] = []
_observed_credentials_lock = threading.Lock()


def _observe_credentials(credentials: ScopeCredentials) -> None:
    """Record a credentials object so the signal handler can read its preferences."""
    with _observed_credentials_lock:
        for existing in _observed_credentials:
            if existing is credentials:
                return
        _observed_credentials.append(credentials)


def _any_observed_cancel_on_shutdown_enabled() -> bool:
    with _observed_credentials_lock:
        if not _observed_credentials:
            return True
        return any(getattr(c, "cancel_jobs_on_shutdown", True) for c in _observed_credentials)


def _observed_max_wait_on_cancel_seconds() -> int:
    with _observed_credentials_lock:
        values = [
            getattr(c, "wait_on_cancel_seconds", DEFAULT_WAIT_ON_CANCEL_SECONDS)
            for c in _observed_credentials
            if getattr(c, "cancel_jobs_on_shutdown", True)
        ]
    return max(values) if values else DEFAULT_WAIT_ON_CANCEL_SECONDS


def _install_signal_handlers() -> None:
    """Install SIGTERM/SIGINT handlers that trigger graceful shutdown.

    Safe to call from any thread — only installs handlers when called from
    the main thread. Subsequent calls are no-ops.
    """
    global _signal_handlers_installed
    if _signal_handlers_installed:
        return
    with _signal_lock:
        if _signal_handlers_installed:
            return
        if threading.current_thread() is not threading.main_thread():
            return
        _prev_sigterm = signal.getsignal(signal.SIGTERM)
        _prev_sigint = signal.getsignal(signal.SIGINT)

        def _handler(signum: int, frame: Any) -> None:
            sig_name = signal.Signals(signum).name
            log.info(f"Received {sig_name} — requesting graceful shutdown")
            _shutdown_event.set()
            if _any_observed_cancel_on_shutdown_enabled():
                try:
                    cancel_all_active_jobs(
                        f"signal:{sig_name}",
                        wait_seconds=_observed_max_wait_on_cancel_seconds(),
                    )
                except Exception as exc:
                    log.warning(f"cancel_all_active_jobs failed in signal handler: {exc}")
            prev = _prev_sigterm if signum == signal.SIGTERM else _prev_sigint
            if callable(prev) and prev not in (signal.SIG_DFL, signal.SIG_IGN):
                prev(signum, frame)

        signal.signal(signal.SIGTERM, _handler)
        signal.signal(signal.SIGINT, _handler)
        _signal_handlers_installed = True


def _atexit_cancel_all() -> None:
    """Fallback cancel-all invoked on interpreter shutdown.

    Covers paths where dbt unwinds via an unhandled exception that does not
    pass through our signal handler.
    """
    if not _any_observed_cancel_on_shutdown_enabled():
        return
    try:
        cancel_all_active_jobs(
            "atexit",
            wait_seconds=_observed_max_wait_on_cancel_seconds(),
        )
    except Exception as exc:
        log.warning(f"cancel_all_active_jobs failed in atexit hook: {exc}")


def _register_atexit() -> None:
    global _atexit_registered
    if _atexit_registered:
        return
    atexit.register(_atexit_cancel_all)
    _atexit_registered = True


def _scope_open_hook(credentials: ScopeCredentials) -> None:
    """Invoked by ``ScopeConnectionManager.open()`` for every connection."""
    _observe_credentials(credentials)
    _install_signal_handlers()
    _register_atexit()


ScopeConnectionManager._on_open = staticmethod(_scope_open_hook)

# Install signal handlers eagerly at module-load time. ``dbt.adapters.scope.impl``
# is imported during dbt's main-thread CLI bootstrap (before any worker threads
# are spawned for model execution), so this is the only reliable place to win
# the race against ``signal.signal()``'s main-thread-only requirement.
# ``ScopeConnectionManager.open()`` runs on per-model worker threads (via
# dbt's ``LazyHandle(self.open)``), where ``signal.signal()`` would raise
# ``ValueError: signal only works in main thread of the main interpreter``
# and our ``_install_signal_handlers`` guard would early-return.
_install_signal_handlers()
_register_atexit()


_TIMESTAMP_COLS = ("accessTime", "modificationTime", "msExpirationTime", "expiryTime")
_SIZE_COLS = ("length", "blockSize")


def _epoch_ms_to_iso(epoch_ms: int | float | None) -> str | None:
    """Convert epoch-millisecond timestamp to ISO-8601 string."""
    if epoch_ms is None or pd.isna(epoch_ms):
        return None
    try:
        return datetime.fromtimestamp(int(epoch_ms) / 1000, tz=timezone.utc).isoformat()
    except (OSError, ValueError):
        return str(epoch_ms)


def _format_bytes(size: int | float | None) -> str:
    """Human-readable byte size."""
    if size is None or pd.isna(size):
        return "N/A"
    s = float(size)
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if abs(s) < 1024:
            return f"{s:.2f} {unit}"
        s /= 1024
    return f"{s:.2f} PB"


def _build_file_df(files: list[FileInfo]) -> pd.DataFrame | None:
    """Build a DataFrame of file metadata from FileInfo objects.

    Returns ``None`` if no raw metadata is available.
    """
    raw_entries = [f.raw for f in files if f.raw]
    if not raw_entries:
        return None

    df = pd.DataFrame(raw_entries)

    if "name" in df.columns:
        df.insert(0, "shortName", df["name"].apply(lambda n: n.rsplit("/", 1)[-1]))

    for col in _TIMESTAMP_COLS:
        if col in df.columns:
            df[f"{col}_utc"] = df[col].apply(_epoch_ms_to_iso)

    for col in _SIZE_COLS:
        if col in df.columns:
            df[f"{col}_fmt"] = df[col].apply(_format_bytes)

    # Byte-estimation columns from enriched FileInfo
    files_with_raw = [f for f in files if f.raw]
    df["estimatedBytes"] = [
        f.estimated_bytes if f.estimated_bytes is not None else f.length for f in files_with_raw
    ]
    df["estimatedBytes_fmt"] = df["estimatedBytes"].apply(_format_bytes)
    df["contributingFiles"] = [list(f.contributing_files) for f in files_with_raw]

    return df


def _tabulate_df(df: pd.DataFrame) -> str:
    """Render a DataFrame as a psql-formatted table via tabulate."""
    return tabulate_lib.tabulate(df, headers="keys", tablefmt="psql", showindex=False)


def _pretty_print_file_tables(
    batch: list[FileInfo],
    backlog: list[FileInfo],
) -> str:
    """Build pretty-printed tables for the current batch and backlog."""
    parts: list[str] = []

    batch_df = _build_file_df(batch)
    if batch_df is not None:
        parts.append(f"=== CURRENT BATCH ({len(batch)} files) ===")
        parts.append(_tabulate_df(batch_df))
    else:
        parts.append(f"=== CURRENT BATCH ({len(batch)} files — no raw metadata) ===")

    if backlog:
        backlog_df = _build_file_df(backlog)
        if backlog_df is not None:
            parts.append(f"\n=== BACKLOG ({len(backlog)} files remaining) ===")
            parts.append(_tabulate_df(backlog_df))
        else:
            parts.append(f"\n=== BACKLOG ({len(backlog)} files — no raw metadata) ===")
    else:
        parts.append("\n=== BACKLOG (0 files remaining) ===")

    return "\n".join(parts)


def _parse_starting_timestamp(value: str) -> datetime:
    """Parse an ISO-8601 UTC timestamp string, raising on bad input.

    Returns a timezone-aware ``datetime`` in UTC.
    """
    try:
        dt = datetime.fromisoformat(value)
    except (ValueError, TypeError) as exc:
        raise DbtRuntimeError(
            f"Invalid starting_timestamp '{value}'. "
            f"Expected an ISO-8601 UTC string such as '2026-04-07T10:00:00+00:00'."
        ) from exc

    if dt.tzinfo is None:
        raise DbtRuntimeError(
            f"starting_timestamp '{value}' is missing timezone info. "
            f"Use an explicit UTC offset, e.g. '2026-04-07T10:00:00+00:00'."
        )

    return dt.astimezone(timezone.utc)


def _count_batches(
    files: list[FileInfo],
    max_files_per_trigger: int,
    max_bytes_per_trigger: int,
) -> int:
    """Simulate batching to count total batches without consuming the file list."""
    remaining = files
    count = 0
    while remaining:
        batch = FileTracker.get_next_batch(remaining, max_files_per_trigger, max_bytes_per_trigger)
        if not batch:
            break
        count += 1
        remaining = remaining[len(batch) :]
    return count


class ScopeAdapter(BaseAdapter):
    """Adapter for submitting SCOPE scripts to Azure Data Lake Analytics."""

    ConnectionManager = ScopeConnectionManager
    Relation = ScopeRelation
    Column = ScopeColumn

    # ------------------------------------------------------------------
    # Required abstract method implementations
    # ------------------------------------------------------------------

    @classmethod
    def date_function(cls) -> str:
        return 'DateTime.UtcNow.ToString("yyyy-MM-dd")'

    @classmethod
    def is_cancelable(cls) -> bool:
        return True

    def list_schemas(self, database: str) -> list[str]:
        """Return the single 'schema' — the container path."""
        creds = self._credentials()
        return [creds.container]

    def check_schema_exists(self, database: str, schema: str) -> bool:
        return schema == self._credentials().container

    def create_schema(self, relation: ScopeRelation) -> None:
        pass  # No-op: SCOPE has no schema concept

    def drop_schema(self, relation: ScopeRelation) -> None:
        pass  # No-op

    def drop_relation(self, relation: ScopeRelation) -> None:
        """Drop is a no-op for safety — SCOPE Delta tables are not casually dropped."""
        if relation is not None:
            self.cache.drop(relation)

    def truncate_relation(self, relation: ScopeRelation) -> None:
        pass  # No-op for safety

    def rename_relation(self, from_relation: ScopeRelation, to_relation: ScopeRelation) -> None:
        raise DbtRuntimeError(
            "SCOPE does not support renaming Delta tables. Use --full-refresh instead."
        )

    def get_columns_in_relation(self, relation: ScopeRelation) -> list[ScopeColumn]:
        """Return columns for a Delta table.

        For SCOPE, column info comes from the model config (sources.yml)
        rather than introspection, since SCOPE has no catalog.
        Returns an empty list — dbt handles this gracefully for custom adapters.
        """
        return []

    def expand_column_types(self, goal: ScopeRelation, current: ScopeRelation) -> None:
        pass  # No-op: SCOPE doesn't support ALTER COLUMN

    def list_relations_without_caching(self, schema_relation: ScopeRelation) -> list[ScopeRelation]:
        """Detect existing Delta tables by checking ADLS for ``_delta_log/`` directories.

        Scans ``{delta_base_path}/`` for subdirectories that contain a
        ``_delta_log/`` folder, which confirms the directory is a Delta table.
        This enables dbt-core's ``_is_incremental()`` check so that microbatch
        runs process only the lookback window instead of all batches from ``begin``.
        """
        creds = self._credentials()
        if not creds.storage_account or not creds.container:
            return []

        message_retry_policy = MessageRetryPolicy.from_credentials(creds)

        try:
            from azure.identity import CredentialUnavailableError
            from azure.storage.filedatalake import DataLakeServiceClient

            t_start = time.monotonic()
            log.debug(
                f"list_relations: scanning {creds.storage_account}/{creds.container}/"
                f"{creds.delta_base_path} for Delta tables"
            )

            credential = build_credential(creds)
            service = DataLakeServiceClient(
                account_url=f"https://{creds.storage_account}.dfs.core.windows.net",
                credential=credential,
            )
            fs = service.get_file_system_client(creds.container)

            t0 = time.monotonic()
            dirs = retry_on_message(
                lambda: [
                    p
                    for p in fs.get_paths(path=creds.delta_base_path, recursive=False)
                    if p.is_directory
                ],
                policy=message_retry_policy,
                label=f"list_relations.get_paths {creds.delta_base_path}",
            )
            elapsed_ms = (time.monotonic() - t0) * 1000
            log.debug(
                f"list_relations: get_paths found {len(dirs)} directories in {elapsed_ms:.1f} ms"
            )

            relations: list[ScopeRelation] = []
            for i, path_info in enumerate(dirs):
                table_name = path_info.name.split("/")[-1]
                t0 = time.monotonic()
                try:

                    def _probe(name=path_info.name):
                        delta_log = fs.get_directory_client(f"{name}/_delta_log")
                        delta_log.get_directory_properties()

                    retry_on_message(
                        _probe,
                        policy=message_retry_policy,
                        label=f"list_relations.probe {table_name}",
                    )
                    relations.append(
                        self.Relation.create(
                            database=creds.storage_account,
                            schema=creds.container,
                            identifier=table_name,
                            type="table",
                        )
                    )
                    elapsed_ms = (time.monotonic() - t0) * 1000
                    log.debug(
                        f"list_relations: [{i + 1}/{len(dirs)}] {table_name} — "
                        f"Delta table found in {elapsed_ms:.1f} ms"
                    )
                except CredentialUnavailableError:
                    raise
                except Exception:
                    elapsed_ms = (time.monotonic() - t0) * 1000
                    log.debug(
                        f"list_relations: [{i + 1}/{len(dirs)}] {table_name} — "
                        f"not a Delta table ({elapsed_ms:.1f} ms)"
                    )

            total_ms = (time.monotonic() - t_start) * 1000
            log.debug(f"list_relations: found {len(relations)} Delta tables in {total_ms:.1f} ms")
            return relations
        except CredentialUnavailableError:
            log.error(
                f"list_relations: credential acquisition exhausted for {creds.delta_base_path}"
            )
            raise
        except Exception:
            log.debug(f"No Delta tables found at {creds.delta_base_path} (path may not exist yet)")
            return []

    def quote(self, identifier: str) -> str:
        return identifier  # SCOPE doesn't use quoted identifiers

    # -- Type conversions (agate → SCOPE) --

    @classmethod
    def convert_text_type(cls, agate_table: agate.Table, col_idx: int) -> str:
        return "string"

    @classmethod
    def convert_number_type(cls, agate_table: agate.Table, col_idx: int) -> str:
        decimals = agate_table.aggregate(agate.HasNulls(col_idx))
        return "double" if decimals else "long"

    @classmethod
    def convert_integer_type(cls, agate_table: agate.Table, col_idx: int) -> str:
        return "long"

    @classmethod
    def convert_boolean_type(cls, agate_table: agate.Table, col_idx: int) -> str:
        return "bool"

    @classmethod
    def convert_datetime_type(cls, agate_table: agate.Table, col_idx: int) -> str:
        return "DateTime"

    @classmethod
    def convert_date_type(cls, agate_table: agate.Table, col_idx: int) -> str:
        return "DateTime"

    @classmethod
    def convert_time_type(cls, agate_table: agate.Table, col_idx: int) -> str:
        return "DateTime"

    # ------------------------------------------------------------------
    # Incremental strategy support
    # ------------------------------------------------------------------

    def valid_incremental_strategies(self) -> list[str]:
        return ["microbatch", "append"]

    # ------------------------------------------------------------------
    # Custom adapter methods (called from macros)
    # ------------------------------------------------------------------

    @available
    def set_next_job_name(self, name: str) -> None:
        """Set the ADLA job name for the next ``execute()`` call on this thread."""
        connection = self.connections.get_thread_connection()
        handle: ScopeConnectionHandle = connection.handle  # type: ignore[assignment]
        handle._next_job_name = name

    @available
    def set_next_job_model_name(self, model_name: str) -> None:
        """Set the dbt model name for the next ``execute()`` call on this thread.

        Used for two purposes:
        1. Orphan cancellation — active ADLA jobs matching this model are cancelled
           before the first job submission (best-effort, once per model per run).
        2. ``related`` metadata — every submitted job carries ``recurrenceId`` and
           ``recurrenceName`` derived from this model name.
        """
        connection = self.connections.get_thread_connection()
        handle: ScopeConnectionHandle = connection.handle  # type: ignore[assignment]
        handle._next_job_model_name = model_name

    @available
    def set_next_job_au(self, au: int) -> None:
        """Set the AU (parallelism) for the next ``execute()`` call on this thread."""
        connection = self.connections.get_thread_connection()
        handle: ScopeConnectionHandle = connection.handle  # type: ignore[assignment]
        handle._next_job_au = au

    @available
    def set_next_job_priority(self, priority: int) -> None:
        """Set the priority for the next ``execute()`` call on this thread."""
        connection = self.connections.get_thread_connection()
        handle: ScopeConnectionHandle = connection.handle  # type: ignore[assignment]
        handle._next_job_priority = priority

    @available
    def get_total_batches(self) -> int:
        """Return the total batch count computed by the last ``discover_files`` call."""
        return getattr(self, "_last_total_batches", 0)

    @available
    def set_next_job_timeout_seconds(self, timeout: int) -> None:
        """Set the job timeout for the next ``execute()`` call on this thread."""
        connection = self.connections.get_thread_connection()
        handle: ScopeConnectionHandle = connection.handle  # type: ignore[assignment]
        handle._next_job_timeout_seconds = timeout

    @available
    def discover_files(
        self,
        source_roots: list[str],
        source_patterns: list[str],
        max_files_per_trigger: int,
        delta_location: str,
        safety_buffer_seconds: int = DEFAULT_SAFETY_BUFFER_SECONDS,
        starting_timestamp: str | None = None,
        max_bytes_per_trigger: int = DEFAULT_MAX_BYTES_PER_TRIGGER,
        starting_timestamp_fallback_to_latest: bool = False,
    ) -> list[str]:
        """Discover unprocessed source files and return a batch of file paths.

        Orchestrates the file-based processing loop across the cross-product
        of *source_roots* x *source_patterns*:
          1. For each (root, pattern): read watermark, LIST + filter files
          2. Union results and deduplicate by file path
          3. Enrich with byte estimates (SSv5/v6 sibling folder detection)
          4. Return files bounded by *max_files_per_trigger* and *max_bytes_per_trigger*

        If *starting_timestamp* is provided (ISO-8601 UTC) and no checkpoint
        exists, only files modified after that timestamp are considered.  When
        a checkpoint already exists the parameter is silently ignored.

        If *starting_timestamp* is after every available source file, the
        behavior depends on *starting_timestamp_fallback_to_latest*: when
        ``False`` (default) a ``DbtRuntimeError`` is raised; when ``True`` the
        single most-recent available file is processed instead (the minimum
        possible lookback), letting developers explore with the least data
        scanned.
        """
        # Validate starting_timestamp early (fail fast on bad input)
        starting_ts_dt = (
            _parse_starting_timestamp(starting_timestamp) if starting_timestamp else None
        )

        tracker = self._get_file_tracker()
        watermark = self._get_checkpoint_manager().read_watermark(delta_location)

        # Determine effective watermark: checkpoint wins over starting_timestamp
        used_starting_timestamp = False
        if watermark is not None:
            effective_watermark = watermark
        elif starting_ts_dt is not None:
            effective_watermark = Watermark(modified_time=starting_ts_dt.isoformat())
            used_starting_timestamp = True
            log.debug(
                f"No checkpoint found — using starting_timestamp={starting_timestamp} "
                f"as initial offset"
            )
        else:
            effective_watermark = None

        seen_paths: set[str] = set()
        all_unprocessed: list[FileInfo] = []

        for root in source_roots:
            for pattern in source_patterns:
                unprocessed = tracker.discover_unprocessed_files(
                    root=root,
                    pattern=pattern,
                    watermark=effective_watermark,
                    safety_buffer_seconds=safety_buffer_seconds,
                )
                for f in unprocessed:
                    if f.path not in seen_paths:
                        seen_paths.add(f.path)
                        all_unprocessed.append(f)

        # If starting_timestamp was used and yielded nothing, either fall back
        # to the single most-recent file (minimum lookback) or raise, depending
        # on starting_timestamp_fallback_to_latest.
        if used_starting_timestamp and not all_unprocessed:
            if starting_timestamp_fallback_to_latest:
                latest = self._find_latest_available_file(
                    tracker, source_roots, source_patterns, safety_buffer_seconds
                )
                if latest is not None:
                    log.warning(
                        "starting_timestamp '%s' is after all available source files; "
                        "falling back to the single most-recent file '%s' "
                        "(modificationTime '%s') because "
                        "starting_timestamp_fallback_to_latest=true.",
                        starting_timestamp,
                        latest.path,
                        latest.modification_time.isoformat(),
                    )
                    all_unprocessed = [latest]
                    seen_paths.add(latest.path)
                # else: no files exist at all — legitimate empty source, no error
            else:
                self._validate_starting_timestamp_has_files(
                    tracker, source_roots, source_patterns, starting_timestamp
                )

        # Sort by modification_time to maintain deterministic ordering
        all_unprocessed.sort(key=lambda f: f.modification_time)

        log.debug(
            f"discover_files: {len(all_unprocessed)} unprocessed files, "
            f"limits: max_files_per_trigger={max_files_per_trigger}, "
            f"max_bytes_per_trigger={_format_bytes(max_bytes_per_trigger)} "
            f"({max_bytes_per_trigger:,} bytes)"
        )

        # Enrich with byte estimates (SSv5/v6 sibling folder detection)
        all_unprocessed = self._get_gen1_client().enrich_with_estimates(all_unprocessed)

        # Cache enriched FileInfo objects for use by update_checkpoint()
        # (cumulative across batch iterations — new files are added, not replaced)
        if not hasattr(self, "_discovered_file_infos"):
            self._discovered_file_infos: dict[str, FileInfo] = {}
        for f in all_unprocessed:
            self._discovered_file_infos[f.path] = f

        # Pre-compute total batch count from the full unprocessed list
        self._last_total_batches = _count_batches(
            all_unprocessed, max_files_per_trigger, max_bytes_per_trigger
        )

        batch = FileTracker.get_next_batch(
            all_unprocessed, max_files_per_trigger, max_bytes_per_trigger
        )

        log.debug(
            f"discover_files: roots={source_roots}, patterns={source_patterns}, "
            f"unprocessed={len(all_unprocessed)}, batch={len(batch)}, "
            f"total_batches={self._last_total_batches}"
        )
        if batch:
            backlog = all_unprocessed[len(batch) :]
            log.debug(f"File discovery results:\n{_pretty_print_file_tables(batch, backlog)}")
        return [f.path for f in batch]

    @staticmethod
    def _validate_starting_timestamp_has_files(
        tracker: FileTracker,
        source_roots: list[str],
        source_patterns: list[str],
        starting_timestamp: str | None,
    ) -> None:
        """Raise if starting_timestamp is after all available source files."""
        for root in source_roots:
            for pattern in source_patterns:
                files = tracker.discover_unprocessed_files(
                    root=root, pattern=pattern, watermark=None, safety_buffer_seconds=0
                )
                if files:
                    raise DbtRuntimeError(
                        f"starting_timestamp '{starting_timestamp}' is after all available "
                        f"source files. The latest file has modificationTime "
                        f"'{files[-1].modification_time.isoformat()}'. "
                        f"Use an earlier timestamp or remove starting_timestamp."
                    )
        # No files exist at all — that's a legitimate empty source, not an error

    @staticmethod
    def _find_latest_available_file(
        tracker: FileTracker,
        source_roots: list[str],
        source_patterns: list[str],
        safety_buffer_seconds: int,
    ) -> FileInfo | None:
        """Return the globally most-recent available source file, or None.

        Re-lists every (root, pattern) with no watermark (still honoring the
        safety buffer) and returns the file with the maximum ``modification_time``.
        Returns ``None`` when the source is genuinely empty.
        """
        latest: FileInfo | None = None
        for root in source_roots:
            for pattern in source_patterns:
                files = tracker.discover_unprocessed_files(
                    root=root,
                    pattern=pattern,
                    watermark=None,
                    safety_buffer_seconds=safety_buffer_seconds,
                )
                for f in files:
                    if latest is None or f.modification_time > latest.modification_time:
                        latest = f
        return latest

    @available
    def update_checkpoint(
        self,
        delta_location: str,
        source_roots: list[str],
        source_patterns: list[str],
        file_paths: list[str],
        source_compaction_interval: int = DEFAULT_SOURCE_COMPACTION_INTERVAL,
        source_retention_files: int = DEFAULT_SOURCE_RETENTION_FILES,
    ) -> None:
        """Update the watermark checkpoint after a successful SCOPE job.

        Uses cached ``FileInfo`` objects from :meth:`discover_files` when
        available, falling back to ADLS listing only for paths not in cache.
        Also writes per-batch JSONL to ``_checkpoint/sources/{batch_id}``,
        triggers compaction at interval boundaries, and enforces retention.
        """
        gen1 = self._get_gen1_client()
        checkpoint = self._get_checkpoint_manager()

        # Get current watermark
        current = checkpoint.read_watermark(delta_location)

        # Look up FileInfo from the discovery cache first
        cache = getattr(self, "_discovered_file_infos", {})
        processed: list[FileInfo] = []
        uncached_paths: set[str] = set()

        for path in file_paths:
            cached_info = cache.get(path)
            if cached_info is not None:
                processed.append(cached_info)
            else:
                uncached_paths.add(path)

        # Fallback: list files from ADLS for any paths not in cache
        if uncached_paths:
            log.debug(
                f"update_checkpoint: {len(uncached_paths)} paths not in cache, "
                f"falling back to ADLS listing"
            )
            seen_paths: set[str] = set()
            for root in source_roots:
                for pattern in source_patterns:
                    all_files = gen1.list_files(root, pattern=pattern)
                    for f in all_files:
                        if f.path in uncached_paths and f.path not in seen_paths:
                            seen_paths.add(f.path)
                            processed.append(f)

        if not processed:
            log.warning("update_checkpoint: no matching files found for paths")
            return

        new_watermark = FileTracker.compute_new_watermark(processed, current)

        # Write watermark
        checkpoint.write_watermark(delta_location, new_watermark)

        # Write per-batch sources (JSONL diff or parquet snapshot at interval)
        checkpoint.write_batch_sources(
            delta_location,
            batch_id=new_watermark.batch_id,
            file_paths=[f.path for f in processed],
            modification_times=[f.modification_time for f in processed],
            compaction_interval=source_compaction_interval,
        )

        # Retention cleanup
        checkpoint.cleanup_sources(
            delta_location,
            max_files=source_retention_files,
        )

    @available
    def delete_checkpoint(self, delta_location: str) -> None:
        """Delete the watermark checkpoint (for full refresh)."""
        self._get_checkpoint_manager().delete_watermark(delta_location)

    @available
    def clear_file_discovery_cache(self) -> None:
        """Clear all file listing and enrichment caches.

        Call between models to force a fresh ADLS listing on the next
        :meth:`discover_files` invocation.
        """
        self._get_gen1_client().clear_file_cache()
        if hasattr(self, "_discovered_file_infos"):
            self._discovered_file_infos.clear()

    @available
    def parse_trigger(self, raw_config: dict | None) -> dict:
        """Parse and validate a trigger config dict from model ``config()``.

        Returns a plain dict suitable for use in Jinja templates with keys:
        ``type``, ``interval_seconds``, ``max_cycles``.
        """
        tc = parse_trigger_config(raw_config)
        return {
            "type": tc.type,
            "interval_seconds": tc.interval.total_seconds(),
            "max_cycles": tc.max_cycles,
        }

    @available
    def compute_schema_evolution(
        self,
        delta_location: str,
        delta_table_columns: list[dict[str, str]],
        partition_by: str | list[str] | None = None,
    ) -> list[dict[str, str]]:
        """Return the columns to ``ALTER TABLE ... ADD COLUMN`` for schema evolution.

        Called from the materialization macros before building the SCOPE script:

          1. If no ``_delta_log`` exists under *delta_location* (new table), return
             ``[]`` — ``CREATE TABLE`` will create the full schema.
          2. Otherwise read the live Delta schema and diff it against
             *delta_table_columns*. Columns present in the table but missing from the
             model, or whose type changed, raise ``DbtRuntimeError``. New columns are
             returned as ``[{"name": .., "type": ..}, ...]`` to be added.

        Args:
            delta_location: ``abfss://`` path to the Delta table.
            delta_table_columns: the model's ``delta_table_columns`` config.
            partition_by: partition column(s), excluded from the diff.
        """
        if not delta_location or not delta_table_columns:
            return []

        client = self._get_delta_client()

        if not client.delta_log_exists(delta_location):
            log.debug(f"compute_schema_evolution: no _delta_log at {delta_location} → new table")
            return []

        existing_schema = client.get_schema(delta_location)
        if existing_schema is None:
            raise DbtRuntimeError(
                f"Delta table at '{delta_location}' has a _delta_log but its schema could "
                f"not be read for schema-evolution checks. Verify the table is a valid Delta "
                f"table and that credentials can read it."
            )

        if isinstance(partition_by, str):
            partition_columns: tuple[str, ...] = (partition_by,)
        elif partition_by:
            partition_columns = tuple(partition_by)
        else:
            partition_columns = ()

        to_add = diff_schema_for_evolution(
            delta_table_columns,
            existing_schema,
            partition_columns=partition_columns,
            location=delta_location,
        )
        if to_add:
            log.info(
                f"SCOPE: schema evolution for {delta_location} — adding "
                f"{len(to_add)} column(s): {', '.join(c['name'] for c in to_add)}"
            )
        return to_add

    @available
    def get_processing_time_timeout(self) -> int:
        """Return the default timeout (seconds) for processing_time models."""
        return DEFAULT_PROCESSING_TIME_TIMEOUT_SECONDS

    @available
    def wait_for_next_cycle(
        self,
        interval_seconds: float,
        max_cycles: int | None = None,
    ) -> bool:
        """Sleep between processing_time cycles; return ``True`` to continue.

        Tracks cycle count per-thread. Returns ``False`` when:
        - A shutdown signal has been received (``_shutdown_event`` is set)
        - ``max_cycles`` has been reached

        The sleep is interruptible — if a shutdown signal arrives during
        ``_shutdown_event.wait()``, it returns immediately.
        """
        _install_signal_handlers()

        thread_id = threading.get_ident()
        if not hasattr(self, "_cycle_counts"):
            self._cycle_counts: dict[int, int] = {}
        self._cycle_counts[thread_id] = self._cycle_counts.get(thread_id, 0) + 1
        cycle = self._cycle_counts[thread_id]

        # Check shutdown before sleeping
        if _shutdown_event.is_set():
            log.info(f"Shutdown requested — exiting after cycle {cycle}")
            self._cycle_counts.pop(thread_id, None)
            return False

        # Check max_cycles
        if max_cycles is not None and cycle >= max_cycles:
            log.info(f"Reached max_cycles={max_cycles} — exiting")
            self._cycle_counts.pop(thread_id, None)
            return False

        log.info(f"Cycle {cycle} complete — sleeping for {interval_seconds}s before next cycle")

        # Interruptible sleep — returns True if the event was set during wait
        interrupted = _shutdown_event.wait(timeout=interval_seconds)
        if interrupted:
            log.info(f"Shutdown requested during sleep — exiting after cycle {cycle}")
            self._cycle_counts.pop(thread_id, None)
            return False

        return True

    @available
    def reset_cycle_count(self) -> None:
        """Reset the cycle counter for the current thread.

        Called at the start of a processing_time model to ensure a fresh count.
        """
        thread_id = threading.get_ident()
        if hasattr(self, "_cycle_counts"):
            self._cycle_counts.pop(thread_id, None)

    @available
    def has_unprocessed_files(
        self,
        source_roots: list[str],
        source_patterns: list[str],
        delta_location: str,
        safety_buffer_seconds: int = DEFAULT_SAFETY_BUFFER_SECONDS,
        starting_timestamp: str | None = None,
        starting_timestamp_fallback_to_latest: bool = False,
    ) -> bool:
        """Are there unprocessed files at the source?"""
        files = self.discover_files(
            source_roots=source_roots,
            source_patterns=source_patterns,
            max_files_per_trigger=1,
            delta_location=delta_location,
            safety_buffer_seconds=safety_buffer_seconds,
            starting_timestamp=starting_timestamp,
            starting_timestamp_fallback_to_latest=starting_timestamp_fallback_to_latest,
        )
        return len(files) > 0

    def build_script_config(self, model_config: dict[str, Any], table_name: str) -> ScriptConfig:
        """Build a ``ScriptConfig`` from dbt model config + credentials."""
        creds = self._credentials()

        # Parse Delta table column definitions
        raw_delta_cols = model_config.get("delta_table_columns", [])
        delta_columns = [
            ColumnDef(
                name=c["name"],
                scope_type=c.get("type", "string"),
            )
            for c in raw_delta_cols
        ]

        # Parse extract column definitions (optional — empty means derive from delta_columns)
        raw_extract_cols = model_config.get("extract_columns", [])
        extract_columns = [
            ColumnDef(
                name=c["name"],
                scope_type=c.get("type", "string"),
            )
            for c in raw_extract_cols
        ]

        return ScriptConfig(
            delta_location=model_config.get("delta_location", ""),
            storage_account=creds.storage_account,
            container=creds.container,
            delta_base_path=creds.delta_base_path,
            table_name=table_name,
            partition_by=model_config.get("partition_by"),
            source_roots=model_config.get("source_roots", []),
            source_patterns=model_config.get("source_patterns", []),
            max_files_per_trigger=model_config.get(
                "max_files_per_trigger", creds.max_files_per_trigger
            ),
            max_bytes_per_trigger=model_config.get(
                "max_bytes_per_trigger", creds.max_bytes_per_trigger
            ),
            safety_buffer_seconds=model_config.get(
                "safety_buffer_seconds", DEFAULT_SAFETY_BUFFER_SECONDS
            ),
            adls_gen1_account=model_config.get("adls_gen1_account", creds.adls_gen1_account),
            scope_settings=model_config.get("scope_settings", {}),
            feature_previews=creds.scope_feature_previews or "EnableDeltaTableDynamicInsert:on",
            au=model_config.get("au", creds.au),
            priority=model_config.get("priority", creds.priority),
            delta_columns=delta_columns,
            extract_columns=extract_columns,
        )

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _credentials(self) -> ScopeCredentials:
        return self.config.credentials  # type: ignore[return-value]

    def _get_gen1_client(self) -> AdlsGen1Client:
        """Return an ADLS Gen1 client for the configured account."""
        if not hasattr(self, "_gen1_client"):
            creds = self._credentials()
            self._gen1_client = AdlsGen1Client(
                account=creds.adls_gen1_account,
                credential=build_credential(creds),
                retry_policy=RetryPolicy.from_http_retries(creds.http_retries),
                message_retry_policy=MessageRetryPolicy.from_credentials(creds),
            )
        return self._gen1_client

    def _get_checkpoint_manager(self) -> CheckpointManager:
        """Return the checkpoint manager singleton."""
        if not hasattr(self, "_checkpoint_manager"):
            creds = self._credentials()
            self._checkpoint_manager = CheckpointManager(
                credential=build_credential(creds),
                retry_policy=RetryPolicy.from_http_retries(creds.http_retries),
                message_retry_policy=MessageRetryPolicy.from_credentials(creds),
            )
        return self._checkpoint_manager

    def _get_file_tracker(self) -> FileTracker:
        """Return the file tracker singleton."""
        if not hasattr(self, "_file_tracker"):
            self._file_tracker = FileTracker(
                gen1_client=self._get_gen1_client(),
                checkpoint_manager=self._get_checkpoint_manager(),
            )
        return self._file_tracker

    def _get_delta_client(self) -> DuckDbDeltaLakeClient:
        """Return the DuckDB-backed Delta Lake client singleton."""
        if not hasattr(self, "_delta_client"):
            creds = self._credentials()
            self._delta_client = DuckDbDeltaLakeClient(
                credential=build_credential(creds),
                message_retry_policy=MessageRetryPolicy.from_credentials(creds),
            )
        return self._delta_client
