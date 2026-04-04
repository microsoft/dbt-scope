"""Tests for ScriptBuilder — pure unit tests, no ADLA calls."""

from dbt.adapters.scope.script_builder import ColumnDef, ScriptBuilder, ScriptConfig


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
        assert "WHERE 1 = 1" in script


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


class TestColumnDef:
    def test_render(self):
        col = ColumnDef(name="my_col", scope_type="string")
        assert col.render() == "    my_col string"
