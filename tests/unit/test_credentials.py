"""Tests for ScopeCredentials."""

from dbt.adapters.scope.credentials import ScopeCredentials

# Base Credentials requires database and schema
_BASE_KWARGS = {"database": "test_db", "schema": "test_schema"}


class TestScopeCredentials:
    def test_type_property(self):
        creds = ScopeCredentials(adla_account="test-account", **_BASE_KWARGS)
        assert creds.type == "scope"

    def test_unique_field(self):
        creds = ScopeCredentials(adla_account="test-account", **_BASE_KWARGS)
        assert creds.unique_field == "test-account"

    def test_connection_keys(self):
        creds = ScopeCredentials(adla_account="test-account", **_BASE_KWARGS)
        keys = creds._connection_keys()
        assert "adla_account" in keys
        assert "storage_account" in keys
        assert "container" in keys

    def test_default_values(self):
        creds = ScopeCredentials(**_BASE_KWARGS)
        assert creds.au == 100
        assert creds.priority == 1
        assert creds.poll_interval_seconds == 5
        assert creds.max_wait_seconds == 3600
        assert creds.delta_base_path == "delta"

    def test_custom_values(self):
        creds = ScopeCredentials(
            adla_account="my-adla",
            storage_account="mystorage",
            container="mycontainer",
            au=50,
            priority=2,
            **_BASE_KWARGS,
        )
        assert creds.adla_account == "my-adla"
        assert creds.storage_account == "mystorage"
        assert creds.container == "mycontainer"
        assert creds.au == 50
        assert creds.priority == 2
