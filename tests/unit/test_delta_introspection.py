"""Unit tests for delta_introspection — mocked DuckDB + ADLS."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

from dbt.adapters.scope.delta_introspection import (
    delta_table_exists,
    get_max_partition,
    parse_abfss,
)

DELTA_LOC = "abfss://ctr@acct.dfs.core.windows.net/delta/my_table"


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
    @patch("dbt.adapters.scope.delta_introspection._make_duckdb_conn")
    def test_returns_true_when_table_exists(self, mock_conn_factory):
        mock_conn = MagicMock()
        mock_conn_factory.return_value = mock_conn
        assert delta_table_exists(DELTA_LOC) is True
        mock_conn.execute.assert_called_once()
        mock_conn.close.assert_called_once()

    @patch("dbt.adapters.scope.delta_introspection._make_duckdb_conn")
    def test_returns_false_when_table_missing(self, mock_conn_factory):
        mock_conn = MagicMock()
        mock_conn.execute.side_effect = Exception("table not found")
        mock_conn_factory.return_value = mock_conn
        assert delta_table_exists(DELTA_LOC) is False
        mock_conn.close.assert_called_once()

    @patch("dbt.adapters.scope.delta_introspection._make_duckdb_conn")
    def test_returns_false_on_connection_error(self, mock_conn_factory):
        mock_conn_factory.side_effect = Exception("no network")
        assert delta_table_exists(DELTA_LOC) is False


# -- get_max_partition ----------------------------------------------------


class TestGetMaxPartition:
    @patch("dbt.adapters.scope.delta_introspection._make_duckdb_conn")
    def test_returns_max_value(self, mock_conn_factory):
        mock_conn = MagicMock()
        mock_conn.execute.return_value.fetchone.return_value = ("20260404",)
        mock_conn_factory.return_value = mock_conn
        assert get_max_partition(DELTA_LOC, "event_year_date") == "20260404"
        mock_conn.close.assert_called_once()

    @patch("dbt.adapters.scope.delta_introspection._make_duckdb_conn")
    def test_returns_none_for_empty_table(self, mock_conn_factory):
        mock_conn = MagicMock()
        mock_conn.execute.return_value.fetchone.return_value = (None,)
        mock_conn_factory.return_value = mock_conn
        assert get_max_partition(DELTA_LOC, "event_year_date") is None
        mock_conn.close.assert_called_once()

    @patch("dbt.adapters.scope.delta_introspection._make_duckdb_conn")
    def test_returns_none_on_error(self, mock_conn_factory):
        mock_conn = MagicMock()
        mock_conn.execute.side_effect = Exception("scan failed")
        mock_conn_factory.return_value = mock_conn
        assert get_max_partition(DELTA_LOC, "event_year_date") is None
        mock_conn.close.assert_called_once()

    @patch("dbt.adapters.scope.delta_introspection._make_duckdb_conn")
    def test_returns_none_on_connection_error(self, mock_conn_factory):
        mock_conn_factory.side_effect = Exception("no network")
        assert get_max_partition(DELTA_LOC, "event_year_date") is None

    @patch("dbt.adapters.scope.delta_introspection._make_duckdb_conn")
    def test_converts_int_partition_to_string(self, mock_conn_factory):
        mock_conn = MagicMock()
        mock_conn.execute.return_value.fetchone.return_value = (20260404,)
        mock_conn_factory.return_value = mock_conn
        assert get_max_partition(DELTA_LOC, "event_year_date") == "20260404"
