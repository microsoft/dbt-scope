"""Shared default constants for the dbt-scope adapter.

These are the single source of truth for default configuration values.
The Jinja macro layer mirrors these in ``macros/materializations/defaults.sql``.
"""

DEFAULT_MAX_FILES_PER_TRIGGER: int = 50
DEFAULT_MAX_BYTES_PER_TRIGGER: int = 10_737_418_240_000  # ~10 TB
DEFAULT_SAFETY_BUFFER_SECONDS: int = 30
DEFAULT_SOURCE_COMPACTION_INTERVAL: int = 10
DEFAULT_SOURCE_RETENTION_FILES: int = 100

# Valid values for @@DeltaLakeCommitCondition
VALID_DELTA_LAKE_COMMIT_CONDITIONS: frozenset[str] = frozenset(
    {
        "FailIfConflict",
        "FailIfPartitionConflict",
        "FailIfFileConflict",
    }
)
DEFAULT_DELTA_LAKE_COMMIT_CONDITION: str = "FailIfFileConflict"
