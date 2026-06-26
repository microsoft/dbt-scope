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


class TestMessageRetryFields:
    def test_defaults(self):
        creds = ScopeCredentials(adla_account="x", **_BASE_KWARGS)
        assert creds.retry_on_error_messages == []
        assert creds.max_retries_on_error == 25
        assert creds.initial_wait_on_error_seconds == 1.0
        assert creds.max_wait_on_error_seconds == 30.0

    def test_custom_values_accepted(self):
        creds = ScopeCredentials(
            adla_account="x",
            retry_on_error_messages=["a", "re:b\\d+"],
            max_retries_on_error=10,
            initial_wait_on_error_seconds=2.0,
            max_wait_on_error_seconds=60.0,
            **_BASE_KWARGS,
        )
        assert creds.retry_on_error_messages == ["a", "re:b\\d+"]
        assert creds.max_retries_on_error == 10
        assert creds.initial_wait_on_error_seconds == 2.0
        assert creds.max_wait_on_error_seconds == 60.0

    def test_negative_max_retries_rejected(self):
        with pytest.raises(DbtRuntimeError, match="max_retries_on_error must be >= 0"):
            ScopeCredentials(adla_account="x", max_retries_on_error=-1, **_BASE_KWARGS)

    def test_non_positive_initial_wait_rejected(self):
        with pytest.raises(DbtRuntimeError, match="initial_wait_on_error_seconds must be > 0"):
            ScopeCredentials(adla_account="x", initial_wait_on_error_seconds=0, **_BASE_KWARGS)

    def test_non_positive_max_wait_rejected(self):
        with pytest.raises(DbtRuntimeError, match="max_wait_on_error_seconds must be > 0"):
            ScopeCredentials(adla_account="x", max_wait_on_error_seconds=0, **_BASE_KWARGS)

    def test_initial_greater_than_max_rejected(self):
        with pytest.raises(DbtRuntimeError, match="must be <= max_wait_on_error_seconds"):
            ScopeCredentials(
                adla_account="x",
                initial_wait_on_error_seconds=60,
                max_wait_on_error_seconds=30,
                **_BASE_KWARGS,
            )

    def test_empty_pattern_string_rejected(self):
        with pytest.raises(DbtRuntimeError, match="non-empty strings"):
            ScopeCredentials(adla_account="x", retry_on_error_messages=[""], **_BASE_KWARGS)

    def test_retry_fields_in_connection_keys(self):
        keys = ScopeCredentials(adla_account="x", **_BASE_KWARGS)._connection_keys()
        assert "retry_on_error_messages" in keys
        assert "max_retries_on_error" in keys
        assert "initial_wait_on_error_seconds" in keys
        assert "max_wait_on_error_seconds" in keys


class TestJobRetryFields:
    def test_defaults(self):
        creds = ScopeCredentials(adla_account="x", **_BASE_KWARGS)
        assert creds.enable_job_retry is True
        assert creds.job_retry_on_messages == []
        assert creds.job_retry_max_attempts == 3
        assert creds.job_retry_initial_wait_seconds == 30.0
        assert creds.job_retry_max_wait_seconds == 300.0

    def test_custom_values_accepted(self):
        creds = ScopeCredentials(
            adla_account="x",
            enable_job_retry=False,
            job_retry_on_messages=["re:Operation timed out", "Flaky"],
            job_retry_max_attempts=5,
            job_retry_initial_wait_seconds=10.0,
            job_retry_max_wait_seconds=120.0,
            **_BASE_KWARGS,
        )
        assert creds.enable_job_retry is False
        assert creds.job_retry_on_messages == ["re:Operation timed out", "Flaky"]
        assert creds.job_retry_max_attempts == 5
        assert creds.job_retry_initial_wait_seconds == 10.0
        assert creds.job_retry_max_wait_seconds == 120.0

    def test_zero_attempts_rejected(self):
        with pytest.raises(DbtRuntimeError, match="job_retry_max_attempts must be >= 1"):
            ScopeCredentials(adla_account="x", job_retry_max_attempts=0, **_BASE_KWARGS)

    def test_non_positive_initial_wait_rejected(self):
        with pytest.raises(DbtRuntimeError, match="job_retry_initial_wait_seconds must be > 0"):
            ScopeCredentials(adla_account="x", job_retry_initial_wait_seconds=0, **_BASE_KWARGS)

    def test_non_positive_max_wait_rejected(self):
        with pytest.raises(DbtRuntimeError, match="job_retry_max_wait_seconds must be > 0"):
            ScopeCredentials(adla_account="x", job_retry_max_wait_seconds=0, **_BASE_KWARGS)

    def test_initial_greater_than_max_rejected(self):
        with pytest.raises(DbtRuntimeError, match="must be <= job_retry_max_wait_seconds"):
            ScopeCredentials(
                adla_account="x",
                job_retry_initial_wait_seconds=300,
                job_retry_max_wait_seconds=30,
                **_BASE_KWARGS,
            )

    def test_empty_pattern_string_rejected(self):
        with pytest.raises(DbtRuntimeError, match="non-empty strings"):
            ScopeCredentials(adla_account="x", job_retry_on_messages=[""], **_BASE_KWARGS)

    def test_job_retry_fields_in_connection_keys(self):
        keys = ScopeCredentials(adla_account="x", **_BASE_KWARGS)._connection_keys()
        assert "enable_job_retry" in keys
        assert "job_retry_on_messages" in keys
        assert "job_retry_max_attempts" in keys
        assert "job_retry_initial_wait_seconds" in keys
        assert "job_retry_max_wait_seconds" in keys


class TestQuotaEvictionFields:
    def test_defaults(self):
        creds = ScopeCredentials(adla_account="x", **_BASE_KWARGS)
        assert creds.enable_quota_eviction is True
        assert creds.quota_eviction_max_attempts == 25
        assert creds.quota_eviction_cancel_num == 5
        assert creds.quota_eviction_wait_seconds == 30.0
        assert creds.quota_eviction_jitter_seconds == 5.0

    def test_custom_values_accepted(self):
        creds = ScopeCredentials(
            adla_account="x",
            enable_quota_eviction=False,
            quota_eviction_max_attempts=3,
            quota_eviction_cancel_num=10,
            quota_eviction_wait_seconds=60.0,
            quota_eviction_jitter_seconds=0.0,
            **_BASE_KWARGS,
        )
        assert creds.enable_quota_eviction is False
        assert creds.quota_eviction_max_attempts == 3
        assert creds.quota_eviction_cancel_num == 10
        assert creds.quota_eviction_wait_seconds == 60.0
        assert creds.quota_eviction_jitter_seconds == 0.0

    def test_negative_max_attempts_rejected(self):
        with pytest.raises(DbtRuntimeError, match="quota_eviction_max_attempts must be >= 0"):
            ScopeCredentials(adla_account="x", quota_eviction_max_attempts=-1, **_BASE_KWARGS)

    def test_zero_cancel_num_rejected(self):
        with pytest.raises(DbtRuntimeError, match="quota_eviction_cancel_num must be >= 1"):
            ScopeCredentials(adla_account="x", quota_eviction_cancel_num=0, **_BASE_KWARGS)

    def test_non_positive_wait_rejected(self):
        with pytest.raises(DbtRuntimeError, match="quota_eviction_wait_seconds must be > 0"):
            ScopeCredentials(adla_account="x", quota_eviction_wait_seconds=0, **_BASE_KWARGS)

    def test_negative_jitter_rejected(self):
        with pytest.raises(DbtRuntimeError, match="quota_eviction_jitter_seconds must be >= 0"):
            ScopeCredentials(adla_account="x", quota_eviction_jitter_seconds=-1, **_BASE_KWARGS)

    def test_fields_in_connection_keys(self):
        keys = ScopeCredentials(adla_account="x", **_BASE_KWARGS)._connection_keys()
        assert "enable_quota_eviction" in keys
        assert "quota_eviction_max_attempts" in keys
        assert "quota_eviction_cancel_num" in keys
        assert "quota_eviction_wait_seconds" in keys
        assert "quota_eviction_jitter_seconds" in keys
