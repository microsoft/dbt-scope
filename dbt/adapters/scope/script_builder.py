"""ScriptBuilder — generates complete SCOPE scripts from dbt model config.

This is the core engine of the adapter.  It translates dbt model SQL + config
into self-contained SCOPE scripts that can be submitted to ADLA as jobs.
"""

from __future__ import annotations

import textwrap
from dataclasses import dataclass, field
from typing import Any


@dataclass
class ColumnDef:
    """A column definition for a SCOPE Delta table."""

    name: str
    scope_type: str

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

    # SS source
    ss_base_path: str = ""
    ss_path_pattern: str = (
        "/{_date:yyyy}/{_date:MM}/{_date:dd}/{_date:yyyy}{_date:MM}{_date:dd}_{*}_{_serial}.ss"
    )

    # Table properties (declarative)
    scope_settings: dict[str, Any] = field(default_factory=dict)

    # Whether to DELETE the batch partition before INSERT (idempotent replace).
    # Default False (append-only). Set True if you need re-runnable batches.
    delete_before_insert: bool = False

    # Number of days each SCOPE job covers in a microbatch run.
    # Default 1 (one job per day). Set higher to bin-pack partitions:
    #   days_per_batch=15 → a 30-day backlog produces 2 SCOPE jobs.
    days_per_batch: int = 1

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
          4. EXTRACT from SS files (full range — no date filter)
          5. INSERT INTO target from user's SELECT
        """
        parts: list[str] = []
        delta_loc = config.resolved_delta_location

        parts.append(_header_comment("full-refresh", config.table_name))
        parts.append(_set_feature_previews(config.feature_previews))
        parts.append(_declare_paths(delta_loc, config.ss_base_path))
        parts.append(_create_table(config.columns, config.partition_by, "@deltaPath"))
        if config.scope_settings:
            parts.append(_alter_table_properties(config.scope_settings))
        parts.append(_extract_from_ss(config.columns, config.partition_by))
        parts.append(_model_transform_and_insert(model_sql, config.partition_by))

        return "\n".join(parts)

    @staticmethod
    def build_incremental(
        config: ScriptConfig,
        model_sql: str,
        batch_start: str,
        batch_end: str,
    ) -> str:
        """Generate a SCOPE script for a microbatch incremental run.

        Steps:
          1. SET feature previews + DeltaLakeCommitCondition
          2. CREATE TABLE IF NOT EXISTS
          3. ALTER TABLE SET TBLPROPERTIES
          4. DELETE existing data in the batch partition range
          5. EXTRACT from SS files filtered by batch date range
          6. INSERT INTO target from user's SELECT
        """
        parts: list[str] = []
        delta_loc = config.resolved_delta_location

        parts.append(
            _header_comment(
                f"microbatch {batch_start} to {batch_end}",
                config.table_name,
            )
        )
        parts.append(_set_feature_previews(config.feature_previews))
        parts.append('SET @@DeltaLakeCommitCondition = "FailIfPartitionConflict";')
        parts.append("")
        parts.append(_declare_paths(delta_loc, config.ss_base_path, batch_start, batch_end))
        parts.append(_create_table(config.columns, config.partition_by, "@deltaPath"))
        if config.scope_settings:
            parts.append(_alter_table_properties(config.scope_settings))
        if config.delete_before_insert:
            parts.append(_delete_batch_partition(config.partition_by))
        parts.append(_extract_from_ss(config.columns, config.partition_by))
        parts.append(
            _model_transform_and_insert(model_sql, config.partition_by, batch_start, batch_end)
        )

        return "\n".join(parts)

    @staticmethod
    def build_checkpoint(
        config: ScriptConfig,
        event_time_col: str,
    ) -> str:
        """Generate a SCOPE script to query MAX(event_time) from a Delta table.

        The result is output to a temporary SS file that the adapter reads.
        """
        delta_loc = config.resolved_delta_location
        return textwrap.dedent(f"""\
            // Checkpoint query for {config.table_name}
            DECLARE TABLE @target
            LOCATION "{delta_loc}"
            OPTIONS (LAYOUT = DELTA);

            @checkpoint = SELECT MAX({event_time_col}) AS max_event_time FROM @target;

            OUTPUT @checkpoint
            TO SSTREAM "/temp/dbt_scope_checkpoint_{config.table_name}.ss";
        """)

    @staticmethod
    def build_drop(config: ScriptConfig) -> str:
        """Generate a SCOPE script to drop (delete all data from) a Delta table."""
        delta_loc = config.resolved_delta_location
        return textwrap.dedent(f"""\
            // Drop all data from {config.table_name}
            DECLARE TABLE @target
            LOCATION "{delta_loc}"
            OPTIONS (LAYOUT = DELTA);

            DELETE FROM @target WHERE 1 = 1;
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


def _declare_paths(
    delta_loc: str,
    ss_base: str,
    batch_start: str | None = None,
    batch_end: str | None = None,
) -> str:
    lines = [
        f'#DECLARE @deltaPath string = "{delta_loc}";',
        f'#DECLARE @ssBase string = "{ss_base}";',
    ]
    if batch_start and batch_end:
        lines.append(f'#DECLARE @startDate string = "{batch_start}";')
        lines.append(f'#DECLARE @endDate string = "{batch_end}";')
    return "\n".join(lines) + "\n"


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


def _delete_batch_partition(partition_by: str | list[str] | None) -> str:
    pcols = _normalize_partition_by(partition_by)
    if not pcols:
        return ""
    # Only the first (date) partition column drives the batch delete range
    date_col = pcols[0]
    return textwrap.dedent(f"""\
        DECLARE TABLE @target_rw
        LOCATION @deltaPath
        OPTIONS (LAYOUT = DELTA);

        DELETE FROM @target_rw
        WHERE {date_col} >= @startDate.Replace("-", "")
          AND {date_col} < @endDate.Replace("-", "");
    """)


def _extract_from_ss(
    columns: list[ColumnDef],
    partition_by: str | list[str] | None,
) -> str:
    pcols = _normalize_partition_by(partition_by)
    # Only the first partition column (date-derived, e.g. event_year_date) is excluded
    # from EXTRACT — it's derived from _date in the user's SELECT, not present in SS.
    # Additional partition columns (e.g. edition) are real data columns and must be extracted.
    derived_col = pcols[0] if pcols else None

    # Build EXTRACT column list (data columns + virtual columns)
    extract_cols: list[str] = []
    for col in columns:
        if col.name == derived_col:
            continue
        extract_cols.append(f"        {col.name} : {col.scope_type}")
    # Add virtual columns
    extract_cols.append("        _date : DateTime")
    extract_cols.append("        _serial : int")
    extract_cols.append("        _source_file = FILE.URI()")

    col_list = ",\n".join(extract_cols)
    pattern = (
        "/{_date:yyyy}/{_date:MM}/{_date:dd}/{_date:yyyy}{_date:MM}{_date:dd}_{*}_{_serial}.ss"
    )

    return textwrap.dedent(f"""\
        @data =
            EXTRACT
        {col_list}
            FROM @ssBase + "{pattern}"
            USING Extractors.SStream();
    """)


def _model_transform_and_insert(
    model_sql: str,
    partition_by: str | list[str] | None,
    batch_start: str | None = None,
    batch_end: str | None = None,
) -> str:
    # Build the user's transformation as a rowset variable
    # The model_sql is the user's SELECT statement
    parts: list[str] = []

    # Apply date filter for incremental
    where_clause = ""
    if batch_start and batch_end:
        where_clause = textwrap.dedent("""\
            WHERE _date >= DateTime.Parse(@startDate)
              AND _date < DateTime.Parse(@endDate)""")

    # Wrap the user's SELECT with the date filter
    parts.append("@batch_data =")
    # Inject the model SQL — the user writes something like:
    #   SELECT col1, col2, _date.ToString("yyyyMMdd") AS event_year_date
    #   FROM @data
    # We need to ensure the FROM @data and WHERE clause are included
    parts.append(f"    {model_sql.strip()}")
    if where_clause:
        parts.append(f"    {where_clause.strip()}")
    parts.append(";")
    parts.append("")
    parts.append("INSERT INTO @target")
    parts.append("SELECT * FROM @batch_data;")

    return "\n".join(parts)
