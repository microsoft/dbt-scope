"""Unit test configuration and shared fixtures."""

import pytest

from dbt.adapters.scope.script_builder import ColumnDef, ScriptConfig


@pytest.fixture
def sample_columns() -> list[ColumnDef]:
    return [
        ColumnDef(name="col_str", scope_type="string"),
        ColumnDef(name="col_long", scope_type="long"),
        ColumnDef(name="col_dt", scope_type="DateTime"),
        ColumnDef(name="event_year_date", scope_type="string"),
    ]


@pytest.fixture
def sample_extract_columns() -> list[ColumnDef]:
    return [
        ColumnDef(name="col_str", scope_type="string"),
        ColumnDef(name="col_long", scope_type="long"),
        ColumnDef(name="col_dt", scope_type="DateTime"),
    ]


@pytest.fixture
def sample_config(
    sample_columns: list[ColumnDef], sample_extract_columns: list[ColumnDef]
) -> ScriptConfig:
    return ScriptConfig(
        delta_location="abfss://testcontainer@teststorage.dfs.core.windows.net/delta/my_table",
        storage_account="teststorage",
        container="testcontainer",
        delta_base_path="delta",
        table_name="my_table",
        partition_by="event_year_date",
        source_roots=["/shares/test/ss/MyStream"],
        source_patterns=[r".*\.ss$"],
        max_files_per_trigger=50,
        safety_buffer_seconds=30,
        adls_gen1_account="test-adls-gen1",
        source_files=[
            "/shares/test/ss/MyStream/2026/04/01/20260401_010000_0.ss",
            "/shares/test/ss/MyStream/2026/04/01/20260401_020000_0.ss",
        ],
        scope_settings={
            "microsoft.scope.compression": "zstd#11",
            "delta.checkpointInterval": 5,
        },
        feature_previews="EnableDeltaTableDynamicInsert:on",
        au=100,
        priority=1,
        delta_columns=sample_columns,
        extract_columns=sample_extract_columns,
    )


@pytest.fixture
def multi_partition_columns() -> list[ColumnDef]:
    return [
        ColumnDef(name="col_str", scope_type="string"),
        ColumnDef(name="col_long", scope_type="long"),
        ColumnDef(name="edition", scope_type="string"),
        ColumnDef(name="event_year_date", scope_type="string"),
    ]


@pytest.fixture
def multi_partition_extract_columns() -> list[ColumnDef]:
    return [
        ColumnDef(name="col_str", scope_type="string"),
        ColumnDef(name="col_long", scope_type="long"),
        ColumnDef(name="edition", scope_type="string"),
    ]


@pytest.fixture
def multi_partition_config(
    multi_partition_columns: list[ColumnDef],
    multi_partition_extract_columns: list[ColumnDef],
) -> ScriptConfig:
    return ScriptConfig(
        delta_location="abfss://testcontainer@teststorage.dfs.core.windows.net/delta/multi_tbl",
        storage_account="teststorage",
        container="testcontainer",
        delta_base_path="delta",
        table_name="multi_tbl",
        partition_by=["event_year_date", "edition"],
        source_roots=["/shares/test/ss/MyStream"],
        source_patterns=[r".*\.ss$"],
        source_files=[
            "/shares/test/ss/MyStream/2026/04/01/20260401_010000_0.ss",
        ],
        scope_settings={},
        feature_previews="EnableDeltaTableDynamicInsert:on",
        au=100,
        priority=1,
        delta_columns=multi_partition_columns,
        extract_columns=multi_partition_extract_columns,
    )
