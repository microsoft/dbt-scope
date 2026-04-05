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
def sample_config(sample_columns: list[ColumnDef]) -> ScriptConfig:
    return ScriptConfig(
        delta_location="abfss://testcontainer@teststorage.dfs.core.windows.net/delta/my_table",
        storage_account="teststorage",
        container="testcontainer",
        delta_base_path="delta",
        table_name="my_table",
        partition_by="event_year_date",
        ss_base_path="/shares/test/ss/MyStream",
        scope_settings={
            "microsoft.scope.compression": "zstd#11",
            "delta.checkpointInterval": 5,
        },
        feature_previews="EnableDeltaTableDynamicInsert:on",
        au=100,
        priority=1,
        columns=sample_columns,
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
def multi_partition_config(multi_partition_columns: list[ColumnDef]) -> ScriptConfig:
    return ScriptConfig(
        delta_location="abfss://testcontainer@teststorage.dfs.core.windows.net/delta/multi_tbl",
        storage_account="teststorage",
        container="testcontainer",
        delta_base_path="delta",
        table_name="multi_tbl",
        partition_by=["event_year_date", "edition"],
        ss_base_path="/shares/test/ss/MyStream",
        scope_settings={},
        feature_previews="EnableDeltaTableDynamicInsert:on",
        au=100,
        priority=1,
        columns=multi_partition_columns,
    )
