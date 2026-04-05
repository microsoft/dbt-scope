"""Tests for ScopeColumn."""

from dbt.adapters.scope.column import ScopeColumn


class TestScopeColumn:
    def test_translate_type_string(self):
        assert ScopeColumn.translate_type("string") == "text"

    def test_translate_type_long(self):
        assert ScopeColumn.translate_type("long") == "integer"

    def test_translate_type_double(self):
        assert ScopeColumn.translate_type("double") == "float"

    def test_translate_type_datetime(self):
        assert ScopeColumn.translate_type("DateTime") == "datetime"

    def test_translate_type_bool(self):
        assert ScopeColumn.translate_type("bool") == "boolean"

    def test_translate_type_nullable(self):
        assert ScopeColumn.translate_type("int?") == "integer"
        assert ScopeColumn.translate_type("DateTime?") == "datetime"

    def test_translate_type_unknown_defaults_to_text(self):
        assert ScopeColumn.translate_type("SomeCustomType") == "text"

    def test_from_scope_type(self):
        col = ScopeColumn.from_scope_type("my_col", "long")
        assert col.column == "my_col"
        assert col.dtype == "long"

    def test_is_scope_nullable(self):
        col = ScopeColumn.from_scope_type("c", "int?")
        assert col.is_scope_nullable is True

    def test_is_not_scope_nullable(self):
        col = ScopeColumn.from_scope_type("c", "string")
        assert col.is_scope_nullable is False

    def test_scope_type_property(self):
        col = ScopeColumn.from_scope_type("c", "DateTime")
        assert col.scope_type == "DateTime"
