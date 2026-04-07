"""ScopeAdapter — dbt adapter for ADLA SCOPE with Delta table support."""

from __future__ import annotations

import logging
from typing import Any

import agate
from dbt.adapters.base import BaseAdapter, available
from dbt_common.exceptions import DbtRuntimeError

from dbt.adapters.scope.adls_gen1_client import AdlsGen1Client, FileInfo
from dbt.adapters.scope.checkpoint import CheckpointManager
from dbt.adapters.scope.column import ScopeColumn
from dbt.adapters.scope.connections import ScopeConnectionHandle, ScopeConnectionManager
from dbt.adapters.scope.credentials import ScopeCredentials
from dbt.adapters.scope.file_tracker import FileTracker
from dbt.adapters.scope.relation import ScopeRelation
from dbt.adapters.scope.script_builder import ColumnDef, ScriptConfig

log = logging.getLogger(__name__)


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
        return False

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

        try:
            from azure.identity import AzureCliCredential
            from azure.storage.filedatalake import DataLakeServiceClient

            from dbt.adapters.scope.delta_lake import LockedTokenCredential

            credential = LockedTokenCredential(AzureCliCredential())
            service = DataLakeServiceClient(
                account_url=f"https://{creds.storage_account}.dfs.core.windows.net",
                credential=credential,
            )
            fs = service.get_file_system_client(creds.container)

            relations: list[ScopeRelation] = []
            for path_info in fs.get_paths(path=creds.delta_base_path, recursive=False):
                if not path_info.is_directory:
                    continue
                table_name = path_info.name.split("/")[-1]
                try:
                    delta_log = fs.get_directory_client(f"{path_info.name}/_delta_log")
                    delta_log.get_directory_properties()
                    relations.append(
                        self.Relation.create(
                            database=creds.storage_account,
                            schema=creds.container,
                            identifier=table_name,
                            type="table",
                        )
                    )
                except Exception:
                    pass  # Not a Delta table — skip
            log.info("list_relations_without_caching found %d Delta tables", len(relations))
            return relations
        except Exception:
            log.debug("No Delta tables found at %s (path may not exist yet)", creds.delta_base_path)
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
    def discover_files(
        self,
        source_roots: list[str],
        source_patterns: list[str],
        max_files_per_trigger: int,
        delta_location: str,
        safety_buffer_seconds: int = 30,
    ) -> list[str]:
        """Discover unprocessed source files and return a batch of file paths.

        Orchestrates the file-based processing loop across the cross-product
        of *source_roots* x *source_patterns*:
          1. For each (root, pattern): read watermark, LIST + filter files
          2. Union results and deduplicate by file path
          3. Return up to *max_files_per_trigger* file paths
        """
        tracker = self._get_file_tracker()
        watermark = self._get_checkpoint_manager().read_watermark(delta_location)

        seen_paths: set[str] = set()
        all_unprocessed: list[FileInfo] = []

        for root in source_roots:
            for pattern in source_patterns:
                unprocessed = tracker.discover_unprocessed_files(
                    root=root,
                    pattern=pattern,
                    watermark=watermark,
                    safety_buffer_seconds=safety_buffer_seconds,
                )
                for f in unprocessed:
                    if f.path not in seen_paths:
                        seen_paths.add(f.path)
                        all_unprocessed.append(f)

        # Sort by modification_time to maintain deterministic ordering
        all_unprocessed.sort(key=lambda f: f.modification_time)
        batch = FileTracker.get_next_batch(all_unprocessed, max_files_per_trigger)

        log.info(
            "discover_files: roots=%s, patterns=%s, unprocessed=%d, batch=%d",
            source_roots,
            source_patterns,
            len(all_unprocessed),
            len(batch),
        )
        return [f.path for f in batch]

    @available
    def update_checkpoint(
        self,
        delta_location: str,
        source_roots: list[str],
        source_patterns: list[str],
        file_paths: list[str],
        source_compaction_interval: int = 10,
        source_retention_files: int = 100,
    ) -> None:
        """Update the watermark checkpoint after a successful SCOPE job.

        Iterates the cross-product of *source_roots* x *source_patterns* to
        reconstruct ``FileInfo`` objects for the processed files, then writes
        the checkpoint.  Also writes per-batch JSONL to
        ``_checkpoint/sources/{batch_id}``, triggers compaction at interval
        boundaries, and enforces retention.
        """
        gen1 = self._get_gen1_client()
        checkpoint = self._get_checkpoint_manager()

        # Get current watermark
        current = checkpoint.read_watermark(delta_location)

        # Reconstruct FileInfo objects across all rootxpattern combos
        target_paths = set(file_paths)
        seen_paths: set[str] = set()
        processed: list[FileInfo] = []

        for root in source_roots:
            for pattern in source_patterns:
                all_files = gen1.list_files(root, pattern=pattern)
                for f in all_files:
                    if f.path in target_paths and f.path not in seen_paths:
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
    def has_unprocessed_files(
        self,
        source_roots: list[str],
        source_patterns: list[str],
        delta_location: str,
        safety_buffer_seconds: int = 30,
    ) -> bool:
        """Are there unprocessed files at the source?"""
        files = self.discover_files(
            source_roots=source_roots,
            source_patterns=source_patterns,
            max_files_per_trigger=1,
            delta_location=delta_location,
            safety_buffer_seconds=safety_buffer_seconds,
        )
        return len(files) > 0

    def submit_scope_script(
        self,
        script: str,
        job_name: str = "dbt-scope",
        au: int | None = None,
        priority: int | None = None,
    ) -> str:
        """Submit a SCOPE script to ADLA and wait for completion.

        Returns the job ID on success.
        """
        connection = self.connections.get_thread_connection()
        handle: ScopeConnectionHandle = connection.handle  # type: ignore[assignment]
        creds = self._credentials()

        job = handle.submit_and_wait(
            name=job_name,
            script=script,
            au=au or creds.au,
            priority=priority or creds.priority,
            poll_interval=creds.poll_interval_seconds,
            max_wait=creds.max_wait_seconds,
        )
        return job.job_id

    def build_script_config(self, model_config: dict[str, Any], table_name: str) -> ScriptConfig:
        """Build a ``ScriptConfig`` from dbt model config + credentials."""
        creds = self._credentials()

        # Parse column definitions from sources.yml metadata
        raw_columns = model_config.get("columns", [])
        columns = [
            ColumnDef(
                name=c["name"],
                scope_type=c.get("type", "string"),
                extract=c.get("extract", True),
            )
            for c in raw_columns
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
            max_files_per_trigger=model_config.get("max_files_per_trigger", 50),
            safety_buffer_seconds=model_config.get("safety_buffer_seconds", 30),
            adls_gen1_account=model_config.get("adls_gen1_account", creds.adls_gen1_account),
            scope_settings=model_config.get("scope_settings", {}),
            feature_previews=creds.scope_feature_previews or "EnableDeltaTableDynamicInsert:on",
            au=model_config.get("au", creds.au),
            priority=model_config.get("priority", creds.priority),
            columns=columns,
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
            self._gen1_client = AdlsGen1Client(account=creds.adls_gen1_account)
        return self._gen1_client

    def _get_checkpoint_manager(self) -> CheckpointManager:
        """Return the checkpoint manager singleton."""
        if not hasattr(self, "_checkpoint_manager"):
            self._checkpoint_manager = CheckpointManager()
        return self._checkpoint_manager

    def _get_file_tracker(self) -> FileTracker:
        """Return the file tracker singleton."""
        if not hasattr(self, "_file_tracker"):
            self._file_tracker = FileTracker(
                gen1_client=self._get_gen1_client(),
                checkpoint_manager=self._get_checkpoint_manager(),
            )
        return self._file_tracker
