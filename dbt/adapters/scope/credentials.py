"""ScopeCredentials — profiles.yml configuration for the SCOPE adapter."""

from __future__ import annotations

from dataclasses import dataclass

from dbt.adapters.contracts.connection import Credentials


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
    http_timeout_seconds: int = 120
    http_retries: int = 10
    scope_feature_previews: str | None = "EnableDeltaTableDynamicInsert:on"
    delta_lake_commit_condition: str = "FailIfFileConflict"

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
            "delta_lake_commit_condition",
        )
