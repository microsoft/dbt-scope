"""Shared default constants for the dbt-scope adapter.

These are the single source of truth for default configuration values.
The Jinja macro layer mirrors these in ``macros/materializations/defaults.sql``.
"""

DEFAULT_MAX_FILES_PER_TRIGGER: int = 50
DEFAULT_MAX_BYTES_PER_TRIGGER: int = 10_737_418_240_000  # ~10 TB
DEFAULT_SAFETY_BUFFER_SECONDS: int = 30
DEFAULT_SOURCE_COMPACTION_INTERVAL: int = 10
DEFAULT_SOURCE_RETENTION_FILES: int = 100

# SCOPE @@MaxFileCountPerOutputFileSet cap. Compiler upstream allows [1, 1_000_000]
# with a default of 100_000, but Fabric/OneLake clusters often enforce a stricter
# 5_000 ceiling at runtime, so the adapter mirrors that as its safe default and
# always emits the SET explicitly to make the value deterministic.
DEFAULT_MAX_FILE_COUNT_PER_OUTPUT_FILE_SET: int = 5000
MAX_FILE_COUNT_PER_OUTPUT_FILE_SET_MIN: int = 1
MAX_FILE_COUNT_PER_OUTPUT_FILE_SET_MAX: int = 1_000_000

# Valid values for @@DeltaLakeCommitCondition
VALID_DELTA_LAKE_COMMIT_CONDITIONS: frozenset[str] = frozenset(
    {
        "FailIfConflict",
        "FailIfPartitionConflict",
        "FailIfFileConflict",
    }
)
DEFAULT_DELTA_LAKE_COMMIT_CONDITION: str = "FailIfFileConflict"

# Trigger mode constants
DEFAULT_TRIGGER_TYPE: str = "available_now"
DEFAULT_PROCESSING_TIME_TIMEOUT_SECONDS: int = 2_592_000  # 30 days

# Graceful shutdown: on SIGINT/SIGTERM, POST CancelJob for every in-flight ADLA job
# and block until each reaches a terminal state (or wait_on_cancel_seconds elapses
# per job, with cancels running in parallel so the total wall-clock is bounded).
DEFAULT_CANCEL_JOBS_ON_SHUTDOWN: bool = True
DEFAULT_WAIT_ON_CANCEL_SECONDS: int = 30
