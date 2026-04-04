"""ScopeAdapter — dbt adapter for ADLA SCOPE with Delta table support."""

from __future__ import annotations

import logging
from typing import Any

import agate
from dbt.adapters.base import BaseAdapter, available
from dbt_common.exceptions import DbtRuntimeError

from dbt.adapters.scope.column import ScopeColumn
from dbt.adapters.scope.connections import ScopeConnectionHandle, ScopeConnectionManager
from dbt.adapters.scope.credentials import ScopeCredentials
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

            from dbt.adapters.scope._file_lock import AZ_CLI_TOKEN_LOCK, FileLock

            with FileLock(AZ_CLI_TOKEN_LOCK):
                credential = AzureCliCredential()
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
            log.info("list_relations_without_caching failed, returning []", exc_info=True)
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
        return ["microbatch", "append", "delete+insert"]

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
    def get_max_partition_value(self, delta_location: str, partition_col: str) -> str | None:
        """Query ``MAX(partition_col)`` from a Delta table via DuckDB.

        Uses DuckDB's ``delta_scan`` to read the Delta transaction log directly
        from ADLS — no ADLA compute cost.  Returns the max value as a string
        (e.g. ``"20260404"``), or ``None`` if the table is empty or unreadable.
        """
        from dbt.adapters.scope.delta_introspection import get_max_partition

        return get_max_partition(delta_location, partition_col)

    @available
    def get_where_connector(self, model_sql: str) -> str:
        """Return ``'AND'`` if *model_sql* already has a ``WHERE``, else ``'WHERE'``.

        Called from Jinja macros to merge date predicates without
        producing duplicate ``WHERE`` clauses.
        """
        from dbt.adapters.scope.sqlglot_parser import parser

        return "AND" if parser.has_top_level_where(model_sql) else "WHERE"

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
            ColumnDef(name=c["name"], scope_type=c.get("type", "string")) for c in raw_columns
        ]

        return ScriptConfig(
            delta_location=model_config.get("delta_location", ""),
            storage_account=creds.storage_account,
            container=creds.container,
            delta_base_path=creds.delta_base_path,
            table_name=table_name,
            partition_by=model_config.get("partition_by"),
            ss_base_path=model_config.get("ss_source_path", ""),
            scope_settings=model_config.get("scope_settings", {}),
            feature_previews=creds.scope_feature_previews or "EnableDeltaTableDynamicInsert:on",
            au=model_config.get("au", creds.au),
            priority=model_config.get("priority", creds.priority),
            columns=columns,
            delete_before_insert=model_config.get("delete_before_insert", False),
            days_per_batch=model_config.get("days_per_batch", 1),
        )

    # ------------------------------------------------------------------
    # Private helpers
    # ------------------------------------------------------------------

    def _credentials(self) -> ScopeCredentials:
        return self.config.credentials  # type: ignore[return-value]
