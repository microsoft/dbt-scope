"""ScopeCredentials — profiles.yml configuration for the SCOPE adapter."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from dbt.adapters.contracts.connection import Credentials
from dbt_common.exceptions import DbtRuntimeError

from dbt.adapters.scope.constants import (
    DEFAULT_CANCEL_JOBS_ON_SHUTDOWN,
    DEFAULT_MAX_FILE_COUNT_PER_OUTPUT_FILE_SET,
    DEFAULT_WAIT_ON_CANCEL_SECONDS,
)


@dataclass
class ScopeCredentials(Credentials):
    """Connection credentials for ADLA SCOPE.

    Configured in ``profiles.yml``::

        my_project:
          target: dev
          outputs:
            dev:
              type: scope
              adla_account: "{{ env_var('SCOPE_ADLA_ACCOUNT') }}"
              storage_account: "{{ env_var('SCOPE_STORAGE_ACCOUNT') }}"
              container: "{{ env_var('SCOPE_CONTAINER') }}"
              delta_base_path: delta
              au: 100
              priority: 1
              max_files_per_trigger: 50
              max_bytes_per_trigger: 10737418240000  # ~10 TB
              max_file_count_per_output_file_set: 5000  # SCOPE @@MaxFileCountPerOutputFileSet
              cancel_jobs_on_shutdown: true             # cancel in-flight ADLA jobs on SIGINT/SIGTERM
              wait_on_cancel_seconds: 30                # wait per job for ADLA terminal state

              # Optional: plug a custom azure.core.credentials.TokenCredential.
              # Defaults to authentication='cli' which uses az login.
              authentication: token_credential
              credential_class: "fabric_entra_auth.EntraTokenCredential"
              credential_kwargs:
                auth:
                  authentication_method: SNI
                  sni:
                    client_id: <guid>
                    tenant_id: <guid>
                    vault_url: 'https://<vault>.vault.azure.net/'
                    vault_certificate_name: <name>
                    vault_pull_config:
                      authentication_method: azCli
    """

    adla_account: str = ""
    storage_account: str = ""
    container: str = ""
    delta_base_path: str = "delta"
    adls_gen1_account: str = ""
    au: int = 100
    priority: int = 1
    poll_interval_seconds: int = 5
    job_timeout_seconds: int = 36_000
    max_files_per_trigger: int = 50
    max_bytes_per_trigger: int = 10_737_418_240_000  # ~10 TB
    max_file_count_per_output_file_set: int = DEFAULT_MAX_FILE_COUNT_PER_OUTPUT_FILE_SET
    cancel_jobs_on_shutdown: bool = DEFAULT_CANCEL_JOBS_ON_SHUTDOWN
    wait_on_cancel_seconds: int = DEFAULT_WAIT_ON_CANCEL_SECONDS
    http_timeout_seconds: int = 120
    http_retries: int = 10
    scope_feature_previews: str | None = "EnableDeltaTableDynamicInsert:on"
    delta_lake_commit_condition: str = "FailIfFileConflict"

    retry_on_error_messages: list[str] = field(default_factory=list)
    max_retries_on_error: int = 25
    initial_wait_on_error_seconds: float = 1.0
    max_wait_on_error_seconds: float = 30.0

    enable_quota_eviction: bool = True
    quota_eviction_max_attempts: int = 25
    quota_eviction_cancel_num: int = 5
    quota_eviction_wait_seconds: float = 30.0
    quota_eviction_jitter_seconds: float = 5.0

    # "cli" (default — AzureCliCredential) or "token_credential" (dotted-path)
    authentication: str = "cli"
    # Dotted path to a TokenCredential implementation loaded via importlib
    # when authentication='token_credential'.
    credential_class: str | None = None
    credential_kwargs: dict[str, Any] = field(default_factory=dict)

    @property
    def type(self) -> str:
        return "scope"

    @property
    def unique_field(self) -> str:
        return self.adla_account

    def _connection_keys(self) -> tuple[str, ...]:
        return (
            "adla_account",
            "storage_account",
            "container",
            "delta_base_path",
            "adls_gen1_account",
            "au",
            "priority",
            "max_files_per_trigger",
            "max_bytes_per_trigger",
            "max_file_count_per_output_file_set",
            "cancel_jobs_on_shutdown",
            "wait_on_cancel_seconds",
            "delta_lake_commit_condition",
            "retry_on_error_messages",
            "max_retries_on_error",
            "initial_wait_on_error_seconds",
            "max_wait_on_error_seconds",
            "enable_quota_eviction",
            "quota_eviction_max_attempts",
            "quota_eviction_cancel_num",
            "quota_eviction_wait_seconds",
            "quota_eviction_jitter_seconds",
            "authentication",
            "credential_class",
        )

    def __post_init__(self) -> None:
        is_token_credential_auth = (
            isinstance(self.authentication, str)
            and self.authentication.lower() == "token_credential"
        )
        if is_token_credential_auth and not self.credential_class:
            raise DbtRuntimeError(
                "authentication='token_credential' requires `credential_class` "
                "(dotted path to an azure.core.credentials.TokenCredential)."
            )
        if not is_token_credential_auth and (self.credential_class or self.credential_kwargs):
            raise DbtRuntimeError(
                "`credential_class` and `credential_kwargs` are only valid when "
                "authentication='token_credential'."
            )

        if self.max_retries_on_error < 0:
            raise DbtRuntimeError(
                f"max_retries_on_error must be >= 0; got {self.max_retries_on_error}"
            )
        if self.initial_wait_on_error_seconds <= 0:
            raise DbtRuntimeError(
                "initial_wait_on_error_seconds must be > 0; "
                f"got {self.initial_wait_on_error_seconds}"
            )
        if self.max_wait_on_error_seconds <= 0:
            raise DbtRuntimeError(
                f"max_wait_on_error_seconds must be > 0; got {self.max_wait_on_error_seconds}"
            )
        if self.initial_wait_on_error_seconds > self.max_wait_on_error_seconds:
            raise DbtRuntimeError(
                "initial_wait_on_error_seconds must be <= max_wait_on_error_seconds; "
                f"got {self.initial_wait_on_error_seconds} > {self.max_wait_on_error_seconds}"
            )
        for entry in self.retry_on_error_messages:
            if not isinstance(entry, str) or not entry:
                raise DbtRuntimeError(
                    f"retry_on_error_messages entries must be non-empty strings; got {entry!r}"
                )

        if self.quota_eviction_max_attempts < 0:
            raise DbtRuntimeError(
                f"quota_eviction_max_attempts must be >= 0; got {self.quota_eviction_max_attempts}"
            )
        if self.quota_eviction_cancel_num < 1:
            raise DbtRuntimeError(
                f"quota_eviction_cancel_num must be >= 1; got {self.quota_eviction_cancel_num}"
            )
        if self.quota_eviction_wait_seconds <= 0:
            raise DbtRuntimeError(
                f"quota_eviction_wait_seconds must be > 0; got {self.quota_eviction_wait_seconds}"
            )
        if self.quota_eviction_jitter_seconds < 0:
            raise DbtRuntimeError(
                "quota_eviction_jitter_seconds must be >= 0; "
                f"got {self.quota_eviction_jitter_seconds}"
            )
