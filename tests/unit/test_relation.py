"""Tests for ScopeRelation."""

from dbt.adapters.scope.relation import ScopeRelation


class TestScopeRelation:
    def test_render_returns_identifier(self):
        rel = ScopeRelation.create(database="db", schema="schema", identifier="my_table")
        assert rel.render() == "my_table"

    def test_render_empty_identifier(self):
        rel = ScopeRelation.create(database="db", schema="schema", identifier=None)
        assert rel.render() == ""

    def test_delta_location(self):
        rel = ScopeRelation.create(database="db", schema="schema", identifier="my_table")
        loc = rel.delta_location(
            storage_account="mystorage",
            container="mycontainer",
            delta_base_path="delta",
        )
        assert loc == ("abfss://mycontainer@mystorage.dfs.core.windows.net/delta/my_table")

    def test_delta_location_custom_path(self):
        rel = ScopeRelation.create(database="db", schema="schema", identifier="tbl")
        loc = rel.delta_location(
            storage_account="acct",
            container="ctr",
            delta_base_path="custom/path",
        )
        assert "custom/path/tbl" in loc

    def test_quote_character_is_empty(self):
        rel = ScopeRelation.create(database="db", schema="schema", identifier="my_table")
        assert rel.quote_character == ""

    def test_not_renameable(self):
        rel = ScopeRelation.create(database="db", schema="schema", identifier="my_table")
        assert len(rel.renameable_relations) == 0

    def test_not_replaceable(self):
        rel = ScopeRelation.create(database="db", schema="schema", identifier="my_table")
        assert len(rel.replaceable_relations) == 0
