"""ScriptBuilder — generates complete SCOPE scripts from dbt model config.

This is the core engine of the adapter.  It translates dbt model SQL + config
into self-contained SCOPE scripts that can be submitted to ADLA as jobs.

File-based processing: Instead of date-pattern extraction, SCOPE scripts
explicitly list source files (comma-separated) in the EXTRACT FROM clause.
"""

from __future__ import annotations

import logging
import textwrap
from dataclasses import dataclass, field
from typing import Any

from dbt.adapters.scope.checkpoint import VIRTUAL_COLUMNS

log = logging.getLogger(__name__)


@dataclass
class ColumnDef:
    """A column definition for a SCOPE Delta table."""

    name: str
    scope_type: str
    extract: bool = True  # False for computed columns not present in source files

    def render(self) -> str:
        return f"    {self.name} {self.scope_type}"


@dataclass
class ScriptConfig:
    """Configuration bag consumed by the script builder.

    Populated from the dbt model config and profile credentials.
    """

    # Delta table location
    delta_location: str = ""
    storage_account: str = ""
    container: str = ""
    delta_base_path: str = "delta"
    table_name: str = ""

    # Partitioning — single column name or list of column names
    partition_by: str | list[str] | None = None

    # File-based source configuration
    source_root: str = ""
    source_pattern: str = ""
    max_files_per_trigger: int = 50
    safety_buffer_seconds: int = 30
    adls_gen1_account: str = ""

    # Explicit file paths for EXTRACT FROM (populated by FileTracker)
    source_files: list[str] = field(default_factory=list)

    # Table properties (declarative)
    scope_settings: dict[str, Any] = field(default_factory=dict)

    # Feature previews
    feature_previews: str = "EnableDeltaTableDynamicInsert:on"

    # Job settings
    au: int = 100
    priority: int = 1

    # Columns
    columns: list[ColumnDef] = field(default_factory=list)

    @property
    def resolved_delta_location(self) -> str:
        if self.delta_location:
            return self.delta_location
        return (
            f"abfss://{self.container}@{self.storage_account}"
            f".dfs.core.windows.net/{self.delta_base_path}/{self.table_name}"
        )


class ScriptBuilder:
    """Generates complete SCOPE scripts for different materialization modes."""

    # -- Public API ---------------------------------------------------

    @staticmethod
    def build_full_refresh(
        config: ScriptConfig,
        model_sql: str,
    ) -> str:
        """Generate a SCOPE script for a full-refresh (table) materialization.

        Steps:
          1. SET feature previews
          2. CREATE TABLE IF NOT EXISTS ... OPTIONS (LAYOUT = DELTA)
          3. ALTER TABLE SET TBLPROPERTIES (if scope_settings present)
          4. DELETE all existing data
          5. EXTRACT from explicit file list
          6. INSERT INTO target from user's SELECT
        """
        log.info(
            "Building full-refresh script for %s → %s (%d files)",
            config.table_name,
            config.resolved_delta_location,
            len(config.source_files),
        )
        parts: list[str] = []
        delta_loc = config.resolved_delta_location

        parts.append(_header_comment("full-refresh", config.table_name))
        parts.append(_set_feature_previews(config.feature_previews))
        parts.append(_declare_paths(delta_loc))
        parts.append(_create_table(config.columns, config.partition_by, "@deltaPath"))
        if config.scope_settings:
            parts.append(_alter_table_properties(config.scope_settings))

        parts.append(_delete_all_rows())
        parts.append(_extract_from_files(config.columns, config.source_files))
        parts.append(_model_transform_and_insert(model_sql))

        script = "\n".join(parts)
        log.info("Full-refresh script length: %d chars", len(script))
        return script

    @staticmethod
    def build_incremental(
        config: ScriptConfig,
        model_sql: str,
    ) -> str:
        """Generate a SCOPE script for a file-based incremental run.

        Steps:
          1. SET feature previews + DeltaLakeCommitCondition
          2. CREATE TABLE IF NOT EXISTS
          3. ALTER TABLE SET TBLPROPERTIES
          4. EXTRACT from explicit file list
          5. INSERT INTO target from user's SELECT
        """
        log.info(
            "Building incremental script for %s (%d files)",
            config.table_name,
            len(config.source_files),
        )
        parts: list[str] = []
        delta_loc = config.resolved_delta_location

        parts.append(
            _header_comment(
                f"incremental ({len(config.source_files)} files)",
                config.table_name,
            )
        )
        parts.append(_set_feature_previews(config.feature_previews))
        parts.append('SET @@DeltaLakeCommitCondition = "FailIfPartitionConflict";')
        parts.append("")
        parts.append(_declare_paths(delta_loc))
        parts.append(_create_table(config.columns, config.partition_by, "@deltaPath"))
        if config.scope_settings:
            parts.append(_alter_table_properties(config.scope_settings))
        parts.append(_extract_from_files(config.columns, config.source_files))
        parts.append(_model_transform_and_insert(model_sql))

        script = "\n".join(parts)
        log.info("Incremental script length: %d chars", len(script))
        return script

    @staticmethod
    def build_drop(config: ScriptConfig) -> str:
        """Generate a SCOPE script to drop (delete all data from) a Delta table."""
        log.info("Building drop script for %s", config.table_name)
        delta_loc = config.resolved_delta_location
        return textwrap.dedent(f"""\
            // Drop all data from {config.table_name}
            DECLARE TABLE @target
            LOCATION "{delta_loc}"
            OPTIONS (LAYOUT = DELTA);

            DELETE FROM @target WHERE true;
        """)


# -- Private helpers --------------------------------------------------


def _normalize_partition_by(partition_by: str | list[str] | None) -> list[str]:
    """Normalize partition_by to a list of column names (empty list if None)."""
    if partition_by is None:
        return []
    if isinstance(partition_by, str):
        return [partition_by]
    return list(partition_by)


def _header_comment(strategy: str, table_name: str) -> str:
    return textwrap.dedent(f"""\
        // {"=" * 60}
        // Generated by dbt-scope adapter
        // Model: {table_name}
        // Strategy: {strategy}
        // {"=" * 60}
    """)


def _set_feature_previews(previews: str) -> str:
    return f'SET @@FeaturePreviews = "{previews}";\n'


def _declare_paths(delta_loc: str) -> str:
    return f'#DECLARE @deltaPath string = "{delta_loc}";\n'


def _create_table(
    columns: list[ColumnDef],
    partition_by: str | list[str] | None,
    location_var: str,
) -> str:
    col_defs = ",\n".join(c.render() for c in columns)
    parts = [
        "CREATE TABLE IF NOT EXISTS @target (",
        col_defs,
        ")",
    ]
    pcols = _normalize_partition_by(partition_by)
    if pcols:
        parts.append(f"PARTITIONED BY ({', '.join(pcols)})")
    parts.append(f"LOCATION {location_var}")
    parts.append("OPTIONS (LAYOUT = DELTA);\n")
    return "\n".join(parts)


def _alter_table_properties(settings: dict[str, Any]) -> str:
    props = ",\n    ".join(f'"{k}" = {_quote_prop_value(v)}' for k, v in settings.items())
    return f"ALTER TABLE @target SET TBLPROPERTIES (\n    {props}\n);\n"


def _quote_prop_value(value: Any) -> str:
    if isinstance(value, str):
        return f'"{value}"'
    return str(value)


def _delete_all_rows() -> str:
    """Emit a DELETE statement that clears all rows from the Delta table."""
    return textwrap.dedent("""\
        DECLARE TABLE @target_rw
        LOCATION @deltaPath
        OPTIONS (LAYOUT = DELTA);

        DELETE FROM @target_rw WHERE true;
    """)


def _extract_from_files(
    columns: list[ColumnDef],
    source_files: list[str],
) -> str:
    """Build an EXTRACT statement with an explicit comma-separated file list.

    Virtual columns (source_file_uri, etc.) are rendered as ``name = FILE.*()``
    instead of the normal ``name : type`` syntax.
    """
    extract_cols: list[str] = []
    for col in columns:
        if not col.extract:
            continue
        if col.name in VIRTUAL_COLUMNS:
            extract_cols.append(f"        {col.name} = {VIRTUAL_COLUMNS[col.name]}")
        else:
            extract_cols.append(f"        {col.name} : {col.scope_type}")

    col_list = ",\n".join(extract_cols)

    # Build file list (comma-separated, quoted paths)
    file_list = ",\n         ".join(f'"{f}"' for f in source_files)

    return textwrap.dedent(f"""\
        @data =
            EXTRACT
        {col_list}
            FROM {file_list}
            USING Extractors.SStream();
    """)


def _model_transform_and_insert(model_sql: str) -> str:
    parts: list[str] = []

    # Strip trailing semicolons — the template adds its own
    sql = model_sql.strip().rstrip(";").rstrip()

    parts.append("@batch_data =")
    parts.append(f"    {sql};")
    parts.append("")
    parts.append("INSERT INTO @target")
    parts.append("SELECT * FROM @batch_data;")

    return "\n".join(parts)
