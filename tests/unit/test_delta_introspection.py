"""Unit tests for delta_lake — mocked DuckDB + ADLS."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
from dbt_common.exceptions import DbtRuntimeError

from dbt.adapters.scope.delta_lake import (
    DuckDbDeltaLakeClient,
    canonical_type,
    diff_schema_for_evolution,
    parse_abfss,
)

DELTA_LOC = "abfss://ctr@acct.dfs.core.windows.net/delta/my_table"


def make_client(mock_conn: MagicMock | None = None) -> tuple[DuckDbDeltaLakeClient, MagicMock]:
    conn = mock_conn or MagicMock()
    credential = MagicMock()
    credential.get_token.return_value = SimpleNamespace(token="storage-token")
    client = DuckDbDeltaLakeClient(credential=credential, connection_factory=lambda: conn)
    return client, conn


# -- parse_abfss ---------------------------------------------------------


class TestParseAbfss:
    def test_valid_url(self):
        result = parse_abfss(DELTA_LOC)
        assert result == ("ctr", "acct", "delta/my_table")

    def test_invalid_url(self):
        assert parse_abfss("https://not-abfss.com/foo") is None

    def test_empty_string(self):
        assert parse_abfss("") is None


# -- delta_table_exists ---------------------------------------------------


class TestDeltaTableExists:
    def test_returns_true_when_table_exists(self):
        client, mock_conn = make_client()
        assert client.table_exists(DELTA_LOC) is True
        assert any(
            "SELECT 1 FROM delta_scan" in call.args[0] for call in mock_conn.execute.call_args_list
        )
        mock_conn.close.assert_called_once()

    def test_returns_false_when_table_missing(self):
        client, mock_conn = make_client()
        mock_conn.execute.side_effect = [None, None, Exception("table not found")]
        assert client.table_exists(DELTA_LOC) is False
        mock_conn.close.assert_called_once()

    def test_returns_false_on_connection_error(self):
        client = DuckDbDeltaLakeClient(
            credential=MagicMock(),
            connection_factory=lambda: (_ for _ in ()).throw(Exception("no network")),
        )
        assert client.table_exists(DELTA_LOC) is False


# -- get_max_partition ----------------------------------------------------


class TestGetMaxPartition:
    def test_returns_max_value(self):
        client, mock_conn = make_client()
        mock_conn.execute.return_value.fetchone.return_value = ("20260404",)
        assert client.get_max_partition(DELTA_LOC, "event_year_date") == "20260404"
        mock_conn.close.assert_called_once()

    def test_returns_none_for_empty_table(self):
        client, mock_conn = make_client()
        mock_conn.execute.return_value.fetchone.return_value = (None,)
        assert client.get_max_partition(DELTA_LOC, "event_year_date") is None
        mock_conn.close.assert_called_once()

    def test_returns_none_on_error(self):
        client, mock_conn = make_client()
        mock_conn.execute.side_effect = [None, None, Exception("scan failed")]
        assert client.get_max_partition(DELTA_LOC, "event_year_date") is None
        mock_conn.close.assert_called_once()

    def test_returns_none_on_connection_error(self):
        client = DuckDbDeltaLakeClient(
            credential=MagicMock(),
            connection_factory=lambda: (_ for _ in ()).throw(Exception("no network")),
        )
        assert client.get_max_partition(DELTA_LOC, "event_year_date") is None

    def test_converts_int_partition_to_string(self):
        client, mock_conn = make_client()
        mock_conn.execute.return_value.fetchone.return_value = (20260404,)
        assert client.get_max_partition(DELTA_LOC, "event_year_date") == "20260404"


# -- get_delta_columns ----------------------------------------------------


class TestGetDeltaColumns:
    def test_returns_column_names(self):
        client, mock_conn = make_client()
        mock_conn.execute.return_value.description = [
            ("col_a", None),
            ("col_b", None),
            ("event_year_date", None),
        ]
        assert client.get_columns(DELTA_LOC) == ["col_a", "col_b", "event_year_date"]
        mock_conn.close.assert_called_once()

    def test_returns_none_on_error(self):
        client, mock_conn = make_client()
        mock_conn.execute.side_effect = [None, None, Exception("table not found")]
        assert client.get_columns(DELTA_LOC) is None
        mock_conn.close.assert_called_once()

    def test_returns_none_on_connection_error(self):
        client = DuckDbDeltaLakeClient(
            credential=MagicMock(),
            connection_factory=lambda: (_ for _ in ()).throw(Exception("no network")),
        )
        assert client.get_columns(DELTA_LOC) is None


# -- validate_partition_column --------------------------------------------


class TestValidatePartitionColumn:
    def test_passes_when_column_exists(self):
        client, _ = make_client()
        with patch.object(
            client, "get_columns", return_value=["col_a", "event_year_date", "col_b"]
        ):
            client.validate_partition_column(DELTA_LOC, "event_year_date")

    def test_raises_when_column_missing(self):
        client, _ = make_client()
        with (
            patch.object(client, "get_columns", return_value=["col_a", "col_b"]),
            pytest.raises(DbtRuntimeError, match="does not contain column 'event_year_date'"),
        ):
            client.validate_partition_column(DELTA_LOC, "event_year_date")

    def test_skips_when_table_unreadable(self):
        client, _ = make_client()
        with patch.object(client, "get_columns", return_value=None):
            client.validate_partition_column(DELTA_LOC, "event_year_date")

    def test_error_message_includes_available_columns(self):
        client, _ = make_client()
        with (
            patch.object(client, "get_columns", return_value=["col_a", "col_b"]),
            pytest.raises(DbtRuntimeError, match="Available columns: \\['col_a', 'col_b'\\]"),
        ):
            client.validate_partition_column(DELTA_LOC, "event_year_date")


class TestDeltaLogFiles:
    @patch("dbt.adapters.scope.delta_lake.DataLakeServiceClient")
    def test_counts_json_commit_files(self, mock_service_client):
        client, _ = make_client()
        mock_file_system = MagicMock()
        mock_file_system.get_paths.return_value = [
            SimpleNamespace(
                name="delta/my_table/_delta_log/00000000000000000000.json", is_directory=False
            ),
            SimpleNamespace(
                name="delta/my_table/_delta_log/00000000000000000001.json", is_directory=False
            ),
            SimpleNamespace(
                name="delta/my_table/_delta_log/00000000000000000001.checkpoint.parquet",
                is_directory=False,
            ),
        ]
        mock_service_client.return_value.get_file_system_client.return_value = mock_file_system
        assert client.count_delta_log_files(DELTA_LOC) == 2

    def test_returns_zero_for_invalid_path(self):
        client, _ = make_client()
        assert client.count_delta_log_files("https://not-abfss.com/foo") == 0


# -- delta_log_exists -----------------------------------------------------


class TestDeltaLogExists:
    @patch("dbt.adapters.scope.delta_lake.DataLakeServiceClient")
    def test_true_when_json_commit_present(self, mock_service_client):
        client, _ = make_client()
        mock_file_system = MagicMock()
        mock_file_system.get_paths.return_value = [
            SimpleNamespace(
                name="delta/my_table/_delta_log/00000000000000000000.json", is_directory=False
            ),
        ]
        mock_service_client.return_value.get_file_system_client.return_value = mock_file_system
        assert client.delta_log_exists(DELTA_LOC) is True

    @patch("dbt.adapters.scope.delta_lake.DataLakeServiceClient")
    def test_false_when_only_non_json(self, mock_service_client):
        client, _ = make_client()
        mock_file_system = MagicMock()
        mock_file_system.get_paths.return_value = [
            SimpleNamespace(name="delta/my_table/_delta_log/_last_checkpoint", is_directory=False),
        ]
        mock_service_client.return_value.get_file_system_client.return_value = mock_file_system
        assert client.delta_log_exists(DELTA_LOC) is False

    @patch("dbt.adapters.scope.delta_lake.DataLakeServiceClient")
    def test_false_when_listing_raises(self, mock_service_client):
        client, _ = make_client()
        mock_service_client.return_value.get_file_system_client.side_effect = Exception("404")
        assert client.delta_log_exists(DELTA_LOC) is False

    def test_false_for_invalid_path(self):
        client, _ = make_client()
        assert client.delta_log_exists("https://not-abfss.com/foo") is False


# -- get_schema -----------------------------------------------------------


class TestGetSchema:
    def test_returns_name_to_type_mapping(self):
        client, mock_conn = make_client()
        mock_conn.execute.return_value.description = [
            ("a", "VARCHAR"),
            ("b", "BIGINT"),
            ("ts", "TIMESTAMP"),
        ]
        assert client.get_schema(DELTA_LOC) == {"a": "VARCHAR", "b": "BIGINT", "ts": "TIMESTAMP"}
        mock_conn.close.assert_called_once()

    def test_returns_none_on_error(self):
        client, mock_conn = make_client()
        mock_conn.execute.side_effect = [None, None, Exception("table not found")]
        assert client.get_schema(DELTA_LOC) is None
        mock_conn.close.assert_called_once()


# -- canonical_type -------------------------------------------------------


class TestCanonicalType:
    @pytest.mark.parametrize(
        ("a", "b"),
        [
            ("string", "VARCHAR"),
            ("long", "BIGINT"),
            ("int", "INTEGER"),
            ("double", "DOUBLE"),
            ("bool", "BOOLEAN"),
            ("DateTime", "TIMESTAMP"),
            ("DateTime", "TIMESTAMP WITH TIME ZONE"),
            ("decimal", "DECIMAL(18,2)"),
        ],
    )
    def test_scope_and_duckdb_spellings_match(self, a, b):
        assert canonical_type(a) == canonical_type(b) is not None

    def test_nullable_suffix_stripped(self):
        assert canonical_type("long?") == canonical_type("long") == "INT64"

    def test_distinct_widths_differ(self):
        assert canonical_type("int") != canonical_type("long")

    def test_unknown_returns_none(self):
        assert canonical_type("mysterytype") is None
        assert canonical_type("") is None
        assert canonical_type(None) is None


# -- diff_schema_for_evolution --------------------------------------------

_EVOLVE_DBT_COLS = [
    {"name": "a", "type": "string"},
    {"name": "b", "type": "long"},
    {"name": "event_year_date", "type": "string"},
]


class TestDiffSchemaForEvolution:
    DBT_COLS = _EVOLVE_DBT_COLS

    def test_new_column_returned_to_add(self):
        existing = {"a": "VARCHAR", "event_year_date": "VARCHAR"}
        to_add = diff_schema_for_evolution(
            self.DBT_COLS, existing, partition_columns=("event_year_date",)
        )
        assert [c["name"] for c in to_add] == ["b"]

    def test_exact_match_returns_empty(self):
        existing = {"a": "VARCHAR", "b": "BIGINT", "event_year_date": "VARCHAR"}
        assert diff_schema_for_evolution(self.DBT_COLS, existing) == []

    def test_case_insensitive_name_match(self):
        existing = {"A": "VARCHAR", "B": "BIGINT", "EVENT_YEAR_DATE": "VARCHAR"}
        assert diff_schema_for_evolution(self.DBT_COLS, existing) == []

    def test_missing_in_dbt_raises(self):
        existing = {"a": "VARCHAR", "b": "BIGINT", "ghost": "VARCHAR"}
        with pytest.raises(DbtRuntimeError, match="MISSING from the model"):
            diff_schema_for_evolution(self.DBT_COLS, existing)

    def test_type_mismatch_raises(self):
        existing = {"a": "VARCHAR", "b": "VARCHAR"}  # b should be BIGINT
        with pytest.raises(DbtRuntimeError, match="type changed"):
            diff_schema_for_evolution(self.DBT_COLS, existing)

    def test_partition_column_absent_from_scan_is_ignored(self):
        # DuckDB may omit the partition column — it must not be treated as missing or to-add.
        existing = {"a": "VARCHAR", "b": "BIGINT"}
        assert (
            diff_schema_for_evolution(
                self.DBT_COLS, existing, partition_columns=("event_year_date",)
            )
            == []
        )

    def test_error_message_includes_both_schemas(self):
        existing = {"a": "VARCHAR", "b": "BIGINT", "ghost": "VARCHAR"}
        with pytest.raises(DbtRuntimeError) as exc:
            diff_schema_for_evolution(self.DBT_COLS, existing, location="abfss://x")
        msg = str(exc.value)
        assert "Model delta_table_columns:" in msg
        assert "Existing Delta table schema:" in msg
        assert "abfss://x" in msg

    def test_unknown_types_do_not_trigger_mismatch(self):
        # An unmappable type on either side must stay lenient (no spurious failure).
        existing = {"a": "SOME_EXOTIC_TYPE", "b": "BIGINT", "event_year_date": "VARCHAR"}
        assert diff_schema_for_evolution(self.DBT_COLS, existing) == []
