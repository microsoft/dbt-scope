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
        assert creds.job_timeout_seconds == 36_000
        assert creds.delta_base_path == "delta"
        assert creds.max_file_count_per_output_file_set == 5000
        assert creds.cancel_jobs_on_shutdown is True
        assert creds.wait_on_cancel_seconds == 30

    def test_custom_values(self):
        creds = ScopeCredentials(
            adla_account="my-adla",
            storage_account="mystorage",
            container="mycontainer",
            au=50,
            priority=2,
            max_file_count_per_output_file_set=250000,
            cancel_jobs_on_shutdown=False,
            wait_on_cancel_seconds=120,
            **_BASE_KWARGS,
        )
        assert creds.adla_account == "my-adla"
        assert creds.storage_account == "mystorage"
        assert creds.container == "mycontainer"
        assert creds.au == 50
        assert creds.priority == 2
        assert creds.max_file_count_per_output_file_set == 250000
        assert creds.cancel_jobs_on_shutdown is False
        assert creds.wait_on_cancel_seconds == 120

    def test_max_file_count_in_connection_keys(self):
        creds = ScopeCredentials(adla_account="test-account", **_BASE_KWARGS)
        assert "max_file_count_per_output_file_set" in creds._connection_keys()

    def test_shutdown_settings_in_connection_keys(self):
        creds = ScopeCredentials(adla_account="test-account", **_BASE_KWARGS)
        keys = creds._connection_keys()
        assert "cancel_jobs_on_shutdown" in keys
        assert "wait_on_cancel_seconds" in keys
