"""Tests for ScriptBuilder — pure unit tests, no ADLA calls."""

import re

import sqlglot
from sqlglot import exp

from dbt.adapters.scope.script_builder import ColumnDef, ScriptBuilder, ScriptConfig
from dbt.adapters.scope.sqlglot_parser import _normalize_scope_sql


class TestScriptBuilderFullRefresh:
    def test_generates_set_feature_previews(self, sample_config):
        script = ScriptBuilder.build_full_refresh(sample_config, "SELECT * FROM @data")
        assert 'SET @@FeaturePreviews = "EnableDeltaTableDynamicInsert:on"' in script

    def test_generates_declare_paths(self, sample_config):
        script = ScriptBuilder.build_full_refresh(sample_config, "SELECT * FROM @data")
        assert "#DECLARE @deltaPath" in script
        assert "#DECLARE @ssBase" in script
        assert sample_config.resolved_delta_location in script

    def test_generates_create_table(self, sample_config):
        script = ScriptBuilder.build_full_refresh(sample_config, "SELECT * FROM @data")
        assert "CREATE TABLE IF NOT EXISTS @target" in script
        assert "col_str string" in script
        assert "col_long long" in script
        assert "PARTITIONED BY (event_year_date)" in script
        assert "OPTIONS (LAYOUT = DELTA)" in script

    def test_generates_alter_table_properties(self, sample_config):
        script = ScriptBuilder.build_full_refresh(sample_config, "SELECT * FROM @data")
        assert "ALTER TABLE @target SET TBLPROPERTIES" in script
        assert '"microsoft.scope.compression"' in script
        assert '"zstd#11"' in script
        assert '"delta.checkpointInterval" = 5' in script

    def test_multi_prop_single_tblproperties(self, sample_config):
        """Multiple scope_settings must produce a single ALTER TABLE statement."""
        script = ScriptBuilder.build_full_refresh(sample_config, "SELECT * FROM @data")
        assert script.count("ALTER TABLE") == 1
        assert '"microsoft.scope.compression" = "zstd#11"' in script
        assert '"delta.checkpointInterval" = 5' in script

    def test_skips_alter_when_no_settings(self, sample_config):
        sample_config.scope_settings = {}
        script = ScriptBuilder.build_full_refresh(sample_config, "SELECT * FROM @data")
        assert "ALTER TABLE" not in script

    def test_generates_extract(self, sample_config):
        script = ScriptBuilder.build_full_refresh(sample_config, "SELECT * FROM @data")
        assert "EXTRACT" in script
        assert "Extractors.SStream()" in script
        assert "_date : DateTime" in script
        assert "FILE.URI()" in script

    def test_partition_column_excluded_from_extract(self, sample_config):
        script = ScriptBuilder.build_full_refresh(sample_config, "SELECT * FROM @data")
        # event_year_date should NOT appear in EXTRACT (it's derived from _date)
        lines = script.split("\n")
        extract_section = False
        for line in lines:
            if "EXTRACT" in line:
                extract_section = True
            if "USING Extractors" in line:
                extract_section = False
            if extract_section and "event_year_date" in line:
                raise AssertionError("partition column event_year_date should not be in EXTRACT")

    def test_generates_insert(self, sample_config):
        script = ScriptBuilder.build_full_refresh(sample_config, "SELECT * FROM @data")
        assert "INSERT INTO @target" in script
        assert "SELECT * FROM @batch_data" in script

    def test_no_partitioning(self, sample_config):
        sample_config.partition_by = None
        script = ScriptBuilder.build_full_refresh(sample_config, "SELECT * FROM @data")
        assert "PARTITIONED BY" not in script

    def test_generates_delete_all_rows(self, sample_config):
        script = ScriptBuilder.build_full_refresh(sample_config, "SELECT * FROM @data")
        assert "DECLARE TABLE @target_rw" in script
        assert "DELETE FROM @target_rw WHERE true" in script

    def test_delete_appears_between_create_and_extract(self, sample_config):
        script = ScriptBuilder.build_full_refresh(sample_config, "SELECT * FROM @data")
        create_pos = script.index("CREATE TABLE IF NOT EXISTS")
        delete_pos = script.index("DELETE FROM @target_rw WHERE true")
        extract_pos = script.index("EXTRACT")
        assert create_pos < delete_pos < extract_pos


class TestScriptBuilderIncremental:
    def test_generates_batch_declares(self, sample_config):
        script = ScriptBuilder.build_incremental(
            sample_config, "SELECT * FROM @data", "2026-04-01", "2026-04-02"
        )
        assert '#DECLARE @startDate string = "2026-04-01"' in script
        assert '#DECLARE @endDate string = "2026-04-02"' in script

    def test_generates_delete_partition_when_enabled(self, sample_config):
        sample_config.delete_before_insert = True
        script = ScriptBuilder.build_incremental(
            sample_config, "SELECT * FROM @data", "2026-04-01", "2026-04-02"
        )
        assert "DELETE FROM @target_rw" in script
        assert "event_year_date >= @startDate" in script
        assert "event_year_date < @endDate" in script

    def test_no_delete_by_default(self, sample_config):
        assert sample_config.delete_before_insert is False
        script = ScriptBuilder.build_incremental(
            sample_config, "SELECT * FROM @data", "2026-04-01", "2026-04-02"
        )
        assert "DELETE" not in script
        assert "target_rw" not in script
        # INSERT should still be present
        assert "INSERT INTO @target" in script

    def test_generates_date_filter(self, sample_config):
        script = ScriptBuilder.build_incremental(
            sample_config, "SELECT * FROM @data", "2026-04-01", "2026-04-02"
        )
        assert "DateTime.Parse(@startDate)" in script
        assert "DateTime.Parse(@endDate)" in script

    def test_generates_commit_condition(self, sample_config):
        script = ScriptBuilder.build_incremental(
            sample_config, "SELECT * FROM @data", "2026-04-01", "2026-04-02"
        )
        assert "FailIfPartitionConflict" in script

    def test_header_contains_batch_info(self, sample_config):
        script = ScriptBuilder.build_incremental(
            sample_config, "SELECT * FROM @data", "2026-04-01", "2026-04-02"
        )
        assert "microbatch" in script
        assert "2026-04-01" in script

    def test_multi_prop_single_tblproperties(self, sample_config):
        """Multiple scope_settings produce a single ALTER TABLE in incremental scripts."""
        script = ScriptBuilder.build_incremental(
            sample_config, "SELECT * FROM @data", "2026-04-01", "2026-04-02"
        )
        assert script.count("ALTER TABLE") == 1
        assert '"microsoft.scope.compression" = "zstd#11"' in script
        assert '"delta.checkpointInterval" = 5' in script


class TestScriptBuilderMultiPartition:
    """Tests for multi-column partitioning support."""

    def test_multi_partition_creates_partitioned_by(self, multi_partition_config):
        script = ScriptBuilder.build_full_refresh(multi_partition_config, "SELECT * FROM @data")
        assert "PARTITIONED BY (event_year_date, edition)" in script

    def test_multi_partition_excludes_only_date_col_from_extract(self, multi_partition_config):
        """Only the first (date-derived) partition column is excluded from EXTRACT.
        Additional partition columns like 'edition' are real data and must be extracted."""
        script = ScriptBuilder.build_full_refresh(multi_partition_config, "SELECT * FROM @data")
        lines = script.split("\n")
        extract_section = False
        found_edition = False
        for line in lines:
            if "EXTRACT" in line:
                extract_section = True
            if "USING Extractors" in line:
                extract_section = False
            if extract_section:
                if "event_year_date" in line:
                    raise AssertionError("event_year_date should not be in EXTRACT")
                if "edition" in line and ":" in line:
                    found_edition = True
        assert found_edition, "edition should be in EXTRACT (it's a real data column)"

    def test_multi_partition_delete_uses_first_col(self, multi_partition_config):
        multi_partition_config.delete_before_insert = True
        script = ScriptBuilder.build_incremental(
            multi_partition_config, "SELECT * FROM @data", "2026-04-01", "2026-04-02"
        )
        assert "DELETE FROM @target_rw" in script
        # Should use first partition column (event_year_date) for date range
        assert "event_year_date >= @startDate" in script

    def test_multi_partition_incremental_batch(self, multi_partition_config):
        script = ScriptBuilder.build_incremental(
            multi_partition_config, "SELECT * FROM @data", "2026-04-01", "2026-04-02"
        )
        assert "PARTITIONED BY (event_year_date, edition)" in script
        assert "INSERT INTO @target" in script


class TestScriptBuilderDaysPerBatch:
    """Tests for the days_per_batch config."""

    def test_days_per_batch_default_is_one(self, sample_config):
        assert sample_config.days_per_batch == 1

    def test_days_per_batch_set(self, sample_config):
        sample_config.days_per_batch = 15
        assert sample_config.days_per_batch == 15

    def test_wide_date_range_incremental(self, sample_config):
        """With days_per_batch=15, a single script covers 15 days."""
        sample_config.days_per_batch = 15
        script = ScriptBuilder.build_incremental(
            sample_config, "SELECT * FROM @data", "2026-02-01", "2026-02-16"
        )
        assert '#DECLARE @startDate string = "2026-02-01"' in script
        assert '#DECLARE @endDate string = "2026-02-16"' in script
        assert "INSERT INTO @target" in script


class TestScriptBuilderCheckpoint:
    def test_generates_checkpoint_query(self, sample_config):
        script = ScriptBuilder.build_checkpoint(sample_config, "event_year_date")
        assert "MAX(event_year_date)" in script
        assert "DECLARE TABLE @target" in script
        assert sample_config.resolved_delta_location in script
        assert "OUTPUT @checkpoint" in script


class TestScriptBuilderDrop:
    def test_generates_delete_all(self, sample_config):
        script = ScriptBuilder.build_drop(sample_config)
        assert "DELETE FROM @target" in script
        assert "WHERE true" in script


class TestScriptConfig:
    def test_resolved_delta_location_uses_explicit(self):
        cfg = ScriptConfig(delta_location="abfss://explicit/path")
        assert cfg.resolved_delta_location == "abfss://explicit/path"

    def test_resolved_delta_location_builds_from_parts(self):
        cfg = ScriptConfig(
            storage_account="acct",
            container="ctr",
            delta_base_path="delta",
            table_name="my_tbl",
        )
        assert cfg.resolved_delta_location == ("abfss://ctr@acct.dfs.core.windows.net/delta/my_tbl")

    def test_partition_by_as_list(self):
        cfg = ScriptConfig(partition_by=["event_year_date", "edition"])
        assert cfg.partition_by == ["event_year_date", "edition"]

    def test_partition_by_as_string(self):
        cfg = ScriptConfig(partition_by="event_year_date")
        assert cfg.partition_by == "event_year_date"


class TestScriptBuilderFilteredSQL:
    """Tests for model SQL that already contains a WHERE clause."""

    def test_incremental_with_existing_where_uses_and(self, sample_config):
        """When model SQL has WHERE, the date predicate is injected with AND."""
        model_sql = (
            "SELECT col_str, col_long, col_dt,\n"
            '    _date.ToString("yyyyMMdd") AS event_year_date\n'
            "FROM @data\n"
            'WHERE col_str == "hello"'
        )
        script = ScriptBuilder.build_incremental(
            sample_config, model_sql, "2026-04-01", "2026-04-02"
        )
        assert "AND _date >= DateTime.Parse(@startDate)" in script
        assert "AND _date < DateTime.Parse(@endDate)" in script
        # Must NOT have a second WHERE — only the user's WHERE and ANDs
        batch_section = script.split("@batch_data =")[1].split("INSERT INTO")[0]
        where_count = batch_section.upper().count("WHERE")
        assert where_count == 1, f"Expected 1 WHERE, found {where_count}"

    def test_incremental_without_where_uses_where(self, sample_config):
        """When model SQL has no WHERE, date predicate uses WHERE (existing behavior)."""
        model_sql = "SELECT * FROM @data"
        script = ScriptBuilder.build_incremental(
            sample_config, model_sql, "2026-04-01", "2026-04-02"
        )
        assert "WHERE _date >= DateTime.Parse(@startDate)" in script
        assert "AND _date < DateTime.Parse(@endDate)" in script

    def test_full_refresh_with_existing_where_no_date_filter(self, sample_config):
        """Full refresh never injects date filter, even with WHERE in model SQL."""
        model_sql = 'SELECT * FROM @data WHERE col_str == "hello"'
        script = ScriptBuilder.build_full_refresh(sample_config, model_sql)
        assert 'WHERE col_str == "hello"' in script
        assert "DateTime.Parse" not in script

    def test_incremental_with_complex_where(self, sample_config):
        """Model SQL with multi-condition WHERE still gets AND for date predicate."""
        model_sql = (
            'SELECT col_str, col_long\nFROM @data\nWHERE col_str == "hello" AND col_long > 100'
        )
        script = ScriptBuilder.build_incremental(
            sample_config, model_sql, "2026-04-01", "2026-04-02"
        )
        assert 'WHERE col_str == "hello" AND col_long > 100' in script
        assert "AND _date >= DateTime.Parse(@startDate)" in script

    def test_trailing_semicolon_stripped(self, sample_config):
        """Model SQL with trailing semicolon doesn't break injection."""
        model_sql = "SELECT * FROM @data;"
        script = ScriptBuilder.build_incremental(
            sample_config, model_sql, "2026-04-01", "2026-04-02"
        )
        assert "WHERE _date >= DateTime.Parse(@startDate)" in script
        assert "INSERT INTO @target" in script

    def test_no_duplicate_where_via_sqlglot(self, multi_partition_config):
        """Regression: filtered model SQL must produce exactly one WHERE clause.

        Uses sqlglot to parse the generated batch SELECT and assert no
        duplicate WHERE nodes exist — mirrors the ADLA error
        E_CSC_USER_DUPLICATECLAUSES.
        """
        # Exact model SQL from filtered_edition.sql
        model_sql = (
            "SELECT\n"
            "    logical_server_name,\n"
            "    logical_database_name,\n"
            "    edition,\n"
            "    state,\n"
            "    region_name,\n"
            "    max_size_bytes,\n"
            '    _date.ToString("yyyyMMdd") AS event_year_date\n'
            "FROM @data\n"
            'WHERE edition == "Standard"'
        )
        script = ScriptBuilder.build_incremental(
            multi_partition_config, model_sql, "2026-02-01", "2026-03-05"
        )

        # Extract the @batch_data assignment (between "@batch_data =" and ";")
        batch_match = re.search(r"@batch_data\s*=\s*(.*?);", script, re.DOTALL)
        assert batch_match, "Could not find @batch_data assignment in script"
        batch_sql = batch_match.group(1).strip()

        # 1) String-level check: only one WHERE keyword
        where_count = len(re.findall(r"\bWHERE\b", batch_sql, re.IGNORECASE))
        assert where_count == 1, (
            f"Expected exactly 1 WHERE in batch SQL, found {where_count}:\n{batch_sql}"
        )

        # 2) sqlglot parse of normalised batch SQL — must have exactly one Where node
        normalized = _normalize_scope_sql(batch_sql)
        parsed = sqlglot.parse_one(normalized, error_level=sqlglot.ErrorLevel.IGNORE)
        assert isinstance(parsed, exp.Select), f"Expected Select node, got {type(parsed).__name__}"
        where_nodes = list(parsed.find_all(exp.Where))
        assert len(where_nodes) == 1, (
            f"sqlglot found {len(where_nodes)} WHERE nodes, expected 1:\n{batch_sql}"
        )


class TestColumnDef:
    def test_render(self):
        col = ColumnDef(name="my_col", scope_type="string")
        assert col.render() == "    my_col string"
