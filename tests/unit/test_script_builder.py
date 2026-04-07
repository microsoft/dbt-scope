"""Tests for ScriptBuilder — pure unit tests, no ADLA calls."""

from dbt.adapters.scope.script_builder import ColumnDef, ScriptBuilder, ScriptConfig


class TestScriptBuilderFullRefresh:
    def test_generates_set_feature_previews(self, sample_config):
        script = ScriptBuilder.build_full_refresh(sample_config, "SELECT * FROM @data")
        assert 'SET @@FeaturePreviews = "EnableDeltaTableDynamicInsert:on"' in script

    def test_generates_declare_delta_path(self, sample_config):
        script = ScriptBuilder.build_full_refresh(sample_config, "SELECT * FROM @data")
        assert "#DECLARE @deltaPath" in script
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

    def test_generates_extract_from_explicit_files(self, sample_config):
        script = ScriptBuilder.build_full_refresh(sample_config, "SELECT * FROM @data")
        assert "EXTRACT" in script
        assert "Extractors.SStream()" in script
        # Should contain the explicit file paths
        assert "20260401_010000_0.ss" in script
        assert "20260401_020000_0.ss" in script

    def test_extractable_columns_in_extract(self, sample_config):
        """Only columns with extract=True should appear in EXTRACT."""
        script = ScriptBuilder.build_full_refresh(sample_config, "SELECT * FROM @data")
        lines = script.split("\n")
        extract_section = False
        found_cols = set()
        for line in lines:
            if "EXTRACT" in line:
                extract_section = True
            if "USING Extractors" in line:
                extract_section = False
            if extract_section:
                for col in sample_config.columns:
                    if col.name in line and ":" in line:
                        found_cols.add(col.name)
        expected = {c.name for c in sample_config.columns if c.extract}
        assert found_cols == expected, (
            f"EXTRACT columns mismatch: got {found_cols}, expected {expected}"
        )
        # event_year_date should NOT be in EXTRACT (extract=False)
        assert "event_year_date" not in found_cols

    def test_no_virtual_columns_in_extract(self, sample_config):
        """Virtual columns _date, _serial, _source_file should NOT appear."""
        script = ScriptBuilder.build_full_refresh(sample_config, "SELECT * FROM @data")
        extract_start = script.index("EXTRACT")
        extract_end = script.index("USING Extractors")
        extract_section = script[extract_start:extract_end]
        # Check for virtual columns as standalone column definitions
        assert "_date : DateTime" not in extract_section
        assert "_serial : int" not in extract_section
        assert "FILE.URI()" not in extract_section

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

    def test_empty_file_list(self, sample_config):
        """ScriptBuilder should handle an empty file list."""
        sample_config.source_files = []
        script = ScriptBuilder.build_full_refresh(sample_config, "SELECT * FROM @data")
        assert "EXTRACT" in script
        assert "FROM " in script


class TestScriptBuilderIncremental:
    def test_generates_commit_condition(self, sample_config):
        script = ScriptBuilder.build_incremental(sample_config, "SELECT * FROM @data")
        assert "FailIfPartitionConflict" in script

    def test_header_contains_file_count(self, sample_config):
        script = ScriptBuilder.build_incremental(sample_config, "SELECT * FROM @data")
        assert "incremental" in script
        assert "2 files" in script

    def test_generates_extract_from_files(self, sample_config):
        script = ScriptBuilder.build_incremental(sample_config, "SELECT * FROM @data")
        assert "20260401_010000_0.ss" in script
        assert "EXTRACT" in script
        assert "Extractors.SStream()" in script

    def test_no_date_declares(self, sample_config):
        """Incremental should not have @startDate/@endDate declares."""
        script = ScriptBuilder.build_incremental(sample_config, "SELECT * FROM @data")
        assert "@startDate" not in script
        assert "@endDate" not in script

    def test_no_date_filter_in_transform(self, sample_config):
        """No date predicate should be injected into user SQL."""
        script = ScriptBuilder.build_incremental(sample_config, "SELECT * FROM @data")
        assert "DateTime.Parse" not in script

    def test_multi_prop_single_tblproperties(self, sample_config):
        """Multiple scope_settings produce a single ALTER TABLE in incremental scripts."""
        script = ScriptBuilder.build_incremental(sample_config, "SELECT * FROM @data")
        assert script.count("ALTER TABLE") == 1
        assert '"microsoft.scope.compression" = "zstd#11"' in script
        assert '"delta.checkpointInterval" = 5' in script

    def test_no_delete_in_incremental(self, sample_config):
        """Incremental should not have DELETE statements."""
        script = ScriptBuilder.build_incremental(sample_config, "SELECT * FROM @data")
        assert "DELETE" not in script

    def test_insert_present(self, sample_config):
        script = ScriptBuilder.build_incremental(sample_config, "SELECT * FROM @data")
        assert "INSERT INTO @target" in script
        assert "SELECT * FROM @batch_data" in script


class TestScriptBuilderMultiPartition:
    """Tests for multi-column partitioning support."""

    def test_multi_partition_creates_partitioned_by(self, multi_partition_config):
        script = ScriptBuilder.build_full_refresh(multi_partition_config, "SELECT * FROM @data")
        assert "PARTITIONED BY (event_year_date, edition)" in script

    def test_extractable_columns_in_extract(self, multi_partition_config):
        """Only columns with extract=True should be in EXTRACT."""
        script = ScriptBuilder.build_full_refresh(multi_partition_config, "SELECT * FROM @data")
        lines = script.split("\n")
        extract_section = False
        found = set()
        for line in lines:
            if "EXTRACT" in line:
                extract_section = True
            if "USING Extractors" in line:
                extract_section = False
            if extract_section:
                for col in multi_partition_config.columns:
                    if col.name in line and ":" in line:
                        found.add(col.name)
        expected = {c.name for c in multi_partition_config.columns if c.extract}
        assert found == expected

    def test_multi_partition_incremental(self, multi_partition_config):
        script = ScriptBuilder.build_incremental(multi_partition_config, "SELECT * FROM @data")
        assert "PARTITIONED BY (event_year_date, edition)" in script
        assert "INSERT INTO @target" in script


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

    def test_source_files_default_empty(self):
        cfg = ScriptConfig()
        assert cfg.source_files == []

    def test_max_files_per_trigger_default(self):
        cfg = ScriptConfig()
        assert cfg.max_files_per_trigger == 50

    def test_safety_buffer_default(self):
        cfg = ScriptConfig()
        assert cfg.safety_buffer_seconds == 30


class TestScriptBuilderModelSQL:
    """Tests for model SQL handling in the new file-based approach."""

    def test_model_sql_with_where_preserved(self, sample_config):
        """Model SQL with WHERE clause should be preserved as-is."""
        model_sql = 'SELECT * FROM @data WHERE edition == "Standard"'
        script = ScriptBuilder.build_incremental(sample_config, model_sql)
        assert 'WHERE edition == "Standard"' in script
        assert "DateTime.Parse" not in script

    def test_trailing_semicolon_stripped(self, sample_config):
        """Model SQL with trailing semicolon doesn't break injection."""
        model_sql = "SELECT * FROM @data;"
        script = ScriptBuilder.build_incremental(sample_config, model_sql)
        assert "INSERT INTO @target" in script

    def test_model_sql_preserved_verbatim(self, sample_config):
        """User SQL should appear in the script unmodified (minus trailing ;)."""
        model_sql = (
            "SELECT\n"
            "    col_str,\n"
            '    DateTime.UtcNow.ToString("yyyyMMdd") AS event_year_date\n'
            "FROM @data"
        )
        script = ScriptBuilder.build_full_refresh(sample_config, model_sql)
        assert 'DateTime.UtcNow.ToString("yyyyMMdd") AS event_year_date' in script


class TestColumnDef:
    def test_render(self):
        col = ColumnDef(name="my_col", scope_type="string")
        assert col.render() == "    my_col string"

    def test_extract_default_true(self):
        col = ColumnDef(name="my_col", scope_type="string")
        assert col.extract is True

    def test_extract_false(self):
        col = ColumnDef(name="event_year_date", scope_type="string", extract=False)
        assert col.extract is False


class TestVirtualColumns:
    """Tests for FILE.* virtual column support in EXTRACT."""

    def test_source_file_uri_uses_file_uri(self):
        config = ScriptConfig(
            delta_location="abfss://c@a.dfs.core.windows.net/d/t",
            table_name="t",
            source_files=["/shares/test/a.ss"],
            columns=[
                ColumnDef(name="col_a", scope_type="string"),
                ColumnDef(name="source_file_uri", scope_type="string"),
            ],
        )
        script = ScriptBuilder.build_incremental(config, "SELECT * FROM @data")
        assert "source_file_uri = FILE.URI()" in script
        assert "col_a : string" in script

    def test_all_four_virtual_columns(self):
        config = ScriptConfig(
            delta_location="abfss://c@a.dfs.core.windows.net/d/t",
            table_name="t",
            source_files=["/shares/test/a.ss"],
            columns=[
                ColumnDef(name="col_a", scope_type="string"),
                ColumnDef(name="source_file_uri", scope_type="string"),
                ColumnDef(name="source_file_length", scope_type="long"),
                ColumnDef(name="source_file_created", scope_type="DateTime"),
                ColumnDef(name="source_file_modified", scope_type="DateTime"),
            ],
        )
        script = ScriptBuilder.build_full_refresh(config, "SELECT * FROM @data")
        assert "source_file_uri = FILE.URI()" in script
        assert "source_file_length = FILE.LENGTH()" in script
        assert "source_file_created = FILE.CREATED()" in script
        assert "source_file_modified = FILE.MODIFIED()" in script
        # Normal column still uses : syntax
        assert "col_a : string" in script
        # Virtual columns still in CREATE TABLE with normal type syntax
        assert "source_file_uri string" in script

    def test_virtual_columns_in_create_table_normal_syntax(self):
        config = ScriptConfig(
            delta_location="abfss://c@a.dfs.core.windows.net/d/t",
            table_name="t",
            source_files=["/shares/test/a.ss"],
            columns=[
                ColumnDef(name="source_file_uri", scope_type="string"),
            ],
        )
        script = ScriptBuilder.build_incremental(config, "SELECT * FROM @data")
        # CREATE TABLE should have normal syntax
        create_section = script.split("CREATE TABLE IF NOT EXISTS")[1].split("LOCATION")[0]
        assert "source_file_uri string" in create_section

    def test_non_virtual_column_not_treated_as_virtual(self):
        config = ScriptConfig(
            delta_location="abfss://c@a.dfs.core.windows.net/d/t",
            table_name="t",
            source_files=["/shares/test/a.ss"],
            columns=[
                ColumnDef(name="my_custom_col", scope_type="string"),
            ],
        )
        script = ScriptBuilder.build_incremental(config, "SELECT * FROM @data")
        assert "my_custom_col : string" in script
        assert "FILE." not in script
