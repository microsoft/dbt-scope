"""Unit tests for delta_introspection — mocked DuckDB + ADLS."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest
from dbt_common.exceptions import DbtRuntimeError

from dbt.adapters.scope.delta_introspection import (
    delta_table_exists,
    get_delta_columns,
    get_max_partition,
    parse_abfss,
    validate_partition_column,
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


# -- get_delta_columns ----------------------------------------------------


class TestGetDeltaColumns:
    @patch("dbt.adapters.scope.delta_introspection._make_duckdb_conn")
    def test_returns_column_names(self, mock_conn_factory):
        mock_conn = MagicMock()
        mock_conn.execute.return_value.description = [
            ("col_a", None),
            ("col_b", None),
            ("event_year_date", None),
        ]
        mock_conn_factory.return_value = mock_conn
        assert get_delta_columns(DELTA_LOC) == ["col_a", "col_b", "event_year_date"]
        mock_conn.close.assert_called_once()

    @patch("dbt.adapters.scope.delta_introspection._make_duckdb_conn")
    def test_returns_none_on_error(self, mock_conn_factory):
        mock_conn = MagicMock()
        mock_conn.execute.side_effect = Exception("table not found")
        mock_conn_factory.return_value = mock_conn
        assert get_delta_columns(DELTA_LOC) is None
        mock_conn.close.assert_called_once()

    @patch("dbt.adapters.scope.delta_introspection._make_duckdb_conn")
    def test_returns_none_on_connection_error(self, mock_conn_factory):
        mock_conn_factory.side_effect = Exception("no network")
        assert get_delta_columns(DELTA_LOC) is None


# -- validate_partition_column --------------------------------------------


class TestValidatePartitionColumn:
    @patch("dbt.adapters.scope.delta_introspection.get_delta_columns")
    def test_passes_when_column_exists(self, mock_columns):
        mock_columns.return_value = ["col_a", "event_year_date", "col_b"]
        validate_partition_column(DELTA_LOC, "event_year_date")  # should not raise

    @patch("dbt.adapters.scope.delta_introspection.get_delta_columns")
    def test_raises_when_column_missing(self, mock_columns):
        mock_columns.return_value = ["col_a", "col_b"]
        with pytest.raises(DbtRuntimeError, match="does not contain column 'event_year_date'"):
            validate_partition_column(DELTA_LOC, "event_year_date")

    @patch("dbt.adapters.scope.delta_introspection.get_delta_columns")
    def test_skips_when_table_unreadable(self, mock_columns):
        mock_columns.return_value = None
        validate_partition_column(DELTA_LOC, "event_year_date")  # should not raise

    @patch("dbt.adapters.scope.delta_introspection.get_delta_columns")
    def test_error_message_includes_available_columns(self, mock_columns):
        mock_columns.return_value = ["col_a", "col_b"]
        with pytest.raises(DbtRuntimeError, match="Available columns: \\['col_a', 'col_b'\\]"):
            validate_partition_column(DELTA_LOC, "event_year_date")
