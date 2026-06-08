"""Tests for ScopeCredentials."""

import pytest
from dbt_common.exceptions import DbtRuntimeError

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


class TestAuthenticationFields:
    def test_authentication_defaults_to_cli(self):
        creds = ScopeCredentials(adla_account="x", **_BASE_KWARGS)
        assert creds.authentication == "cli"
        assert creds.credential_class is None
        assert creds.credential_kwargs == {}

    def test_token_credential_requires_credential_class(self):
        with pytest.raises(DbtRuntimeError, match="requires `credential_class`"):
            ScopeCredentials(
                adla_account="x",
                authentication="token_credential",
                **_BASE_KWARGS,
            )

    def test_token_credential_accepts_credential_class(self):
        creds = ScopeCredentials(
            adla_account="x",
            authentication="token_credential",
            credential_class="my_pkg.MyCredential",
            credential_kwargs={"foo": "bar"},
            **_BASE_KWARGS,
        )
        assert creds.credential_class == "my_pkg.MyCredential"
        assert creds.credential_kwargs == {"foo": "bar"}

    def test_credential_class_rejected_under_cli_auth(self):
        with pytest.raises(DbtRuntimeError, match="only valid when"):
            ScopeCredentials(
                adla_account="x",
                credential_class="my_pkg.MyCredential",
                **_BASE_KWARGS,
            )

    def test_credential_kwargs_rejected_under_cli_auth(self):
        with pytest.raises(DbtRuntimeError, match="only valid when"):
            ScopeCredentials(
                adla_account="x",
                credential_kwargs={"foo": "bar"},
                **_BASE_KWARGS,
            )

    def test_authentication_in_connection_keys(self):
        creds = ScopeCredentials(adla_account="x", **_BASE_KWARGS)
        keys = creds._connection_keys()
        assert "authentication" in keys
        assert "credential_class" in keys

    def test_authentication_case_insensitive_match(self):
        creds = ScopeCredentials(
            adla_account="x",
            authentication="Token_Credential",
            credential_class="my_pkg.MyCredential",
            **_BASE_KWARGS,
        )
        assert creds.credential_class == "my_pkg.MyCredential"
