"""Shared default constants for the dbt-scope adapter.

These are the single source of truth for default configuration values.
The Jinja macro layer mirrors these in ``macros/materializations/defaults.sql``.
"""

DEFAULT_MAX_FILES_PER_TRIGGER: int = 50
DEFAULT_MAX_BYTES_PER_TRIGGER: int = 107_374_182_400  # 1000 GB
DEFAULT_SAFETY_BUFFER_SECONDS: int = 30
DEFAULT_SOURCE_COMPACTION_INTERVAL: int = 10
DEFAULT_SOURCE_RETENTION_FILES: int = 100
