"""Tests for SqlglotParser — WHERE clause detection for SCOPE model SQL."""

from dbt.adapters.scope.sql_parser import SqlParser
from dbt.adapters.scope.sqlglot_parser import SqlglotParser, parser


class TestSqlglotParserIsAbc:
    """Verify the ABC/singleton structure."""

    def test_parser_is_sql_parser_instance(self):
        assert isinstance(parser, SqlParser)

    def test_parser_is_sqlglot_parser(self):
        assert isinstance(parser, SqlglotParser)


class TestHasTopLevelWhere:
    """Tests for parser.has_top_level_where()."""

    def test_no_where(self):
        sql = "SELECT col1, col2 FROM @data"
        assert parser.has_top_level_where(sql) is False

    def test_simple_where(self):
        sql = 'SELECT col1 FROM @data WHERE edition == "Standard"'
        assert parser.has_top_level_where(sql) is True

    def test_where_with_multiple_conditions(self):
        sql = 'SELECT col1 FROM @data WHERE edition == "Standard" AND region_name == "West US"'
        assert parser.has_top_level_where(sql) is True

    def test_where_in_subquery_only(self):
        sql = 'SELECT * FROM (SELECT col1 FROM @data WHERE edition == "Standard") AS sub'
        assert parser.has_top_level_where(sql) is False

    def test_where_in_string_literal(self):
        sql = "SELECT 'WHERE' AS label FROM @data"
        assert parser.has_top_level_where(sql) is False

    def test_scope_sql_with_tostring(self):
        sql = (
            "SELECT\n"
            "    logical_server_name,\n"
            '    _date.ToString("yyyyMMdd") AS event_year_date\n'
            "FROM @data\n"
            'WHERE edition == "Standard"'
        )
        assert parser.has_top_level_where(sql) is True

    def test_scope_sql_without_where_tostring(self):
        sql = (
            "SELECT\n"
            "    logical_server_name,\n"
            '    _date.ToString("yyyyMMdd") AS event_year_date\n'
            "FROM @data"
        )
        assert parser.has_top_level_where(sql) is False

    def test_multiline_where(self):
        sql = (
            "SELECT col1, col2\n"
            "FROM @data\n"
            "WHERE\n"
            '    edition == "Standard"\n'
            '    AND state == "Online"'
        )
        assert parser.has_top_level_where(sql) is True

    def test_empty_sql(self):
        assert parser.has_top_level_where("") is False

    def test_select_only(self):
        assert parser.has_top_level_where("SELECT 1") is False

    def test_where_case_insensitive(self):
        sql = 'SELECT col1 FROM @data where edition == "Standard"'
        assert parser.has_top_level_where(sql) is True

    def test_where_mixed_case(self):
        sql = 'SELECT col1 FROM @data Where edition == "Standard"'
        assert parser.has_top_level_where(sql) is True
