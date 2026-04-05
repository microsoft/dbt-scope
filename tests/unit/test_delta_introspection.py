"""Unit tests for delta_lake — mocked DuckDB + ADLS."""

from __future__ import annotations

from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
from dbt_common.exceptions import DbtRuntimeError

from dbt.adapters.scope.delta_lake import DuckDbDeltaLakeClient, parse_abfss

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
