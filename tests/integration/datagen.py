"""Datagen — generate synthetic SS test files on Cosmos via ADLA.

Produces a single SCOPE script with VALUES rows and OUTPUT TO SSTREAM
statements for N days x M files_per_day, then submits it as an ADLA job.

Usage as a library::

    from datagen import ScopeColumn, ScopeDataset, generate_scope_script, submit_datagen_job

    dataset = ScopeDataset(
        stream_name="dbt_test_20260404",
        ss_root="/local/mdrrahman/ss",
        columns=[
            ScopeColumn("logical_server_name", "string"),
            ScopeColumn("edition", "string"),
            ScopeColumn("max_size_bytes", "long"),
            ScopeColumn("PreciseTimeStamp", "DateTime"),
        ],
        rows=[
            ('"server-001"', '"Standard"', "1073741824L", None),  # None = auto PreciseTimeStamp
            ('"server-002"', '"Premium"',  "2147483648L", None),
        ],
        start_date="2026-02-01",
        days=60,
        files_per_day=2,
        stream_expiry_days=7,
    )

    script = generate_scope_script(dataset)
    job = submit_datagen_job(dataset, adla_account="sqldb-adhoc-c11")
"""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone

log = logging.getLogger(__name__)


@dataclass
class ScopeColumn:
    """A column in the test dataset."""

    name: str
    scope_type: str


@dataclass
class ScopeDataset:
    """Full specification for a synthetic SS test dataset.

    Attributes:
        stream_name: Directory name under ss_root (e.g. ``dbt_test_20260404``).
        ss_root: Writable Cosmos path prefix (e.g. ``/local/mdrrahman/ss``).
        columns: Column definitions (name + SCOPE type).
        rows: List of value tuples matching ``columns``. Use ``None`` in the
            last position to auto-fill ``PreciseTimeStamp`` with the file's date.
        start_date: First date to generate (ISO format ``YYYY-MM-DD``).
        days: Number of consecutive days to generate.
        files_per_day: Number of SS files per day (serial 0..N-1).
        stream_expiry_days: STREAMEXPIRY in days (auto-cleanup).
    """

    stream_name: str
    ss_root: str
    columns: list[ScopeColumn]
    rows: list[tuple]
    start_date: str = "2026-02-01"
    days: int = 60
    files_per_day: int = 2
    stream_expiry_days: int = 7

    @property
    def ss_base_path(self) -> str:
        """Full Cosmos path to the stream directory."""
        return f"{self.ss_root}/{self.stream_name}"

    @property
    def date_range(self) -> list[date]:
        """List of dates covered by this dataset."""
        start = date.fromisoformat(self.start_date)
        return [start + timedelta(days=i) for i in range(self.days)]

    def expected_rows_per_partition(self) -> dict[str, int]:
        """Return expected row counts keyed by partition value (yyyyMMdd).

        Each day produces ``len(rows) * files_per_day`` rows with partition
        value = the day formatted as ``yyyyMMdd``.
        """
        rows_per_day = len(self.rows) * self.files_per_day
        return {dt.strftime("%Y%m%d"): rows_per_day for dt in self.date_range}

    @property
    def total_expected_rows(self) -> int:
        """Total rows expected across all partitions."""
        return len(self.rows) * self.files_per_day * self.days


def generate_scope_script(dataset: ScopeDataset) -> str:
    """Generate a complete SCOPE script that creates SS files.

    Returns the script text (not submitted — call ``submit_datagen_job`` for that).
    """
    parts: list[str] = []

    # Header
    parts.append(f"// Datagen: {dataset.stream_name}")
    parts.append(
        f"// {dataset.days} days x {dataset.files_per_day} files = "
        f"{dataset.days * dataset.files_per_day} SS files"
    )
    parts.append("")

    # Build VALUES rows
    col_names = [c.name for c in dataset.columns]
    has_timestamp = any(c.name == "PreciseTimeStamp" for c in dataset.columns)

    values_lines: list[str] = []
    for row in dataset.rows:
        vals = []
        for i, v in enumerate(row):
            if v is None and has_timestamp and dataset.columns[i].name == "PreciseTimeStamp":
                vals.append("__TIMESTAMP__")  # placeholder — filled per-day below
            else:
                vals.append(str(v))
        values_lines.append(f"        ( {', '.join(vals)} )")

    col_def = ", ".join(col_names)

    # For each date, generate a @data rowset and OUTPUT statements
    dates = dataset.date_range
    hours = _generate_hours(dataset.files_per_day)

    for dt in dates:
        date_str = dt.strftime("%Y-%m-%d")
        ts_literal = f'DateTime.Parse("{date_str} 19:00:00")'

        # Build VALUES with timestamp filled in
        filled_lines = [line.replace("__TIMESTAMP__", ts_literal) for line in values_lines]
        filled_values = ",\n".join(filled_lines)

        rowset_name = f"@data_{dt.strftime('%Y%m%d')}"
        parts.append(f"{rowset_name} =")
        parts.append("    SELECT *")
        parts.append("    FROM (")
        parts.append("        VALUES")
        parts.append(filled_values)
        parts.append(f"    ) AS T ({col_def});")
        parts.append("")

        # OUTPUT statements for each file in this day
        for serial, hour in enumerate(hours):
            yyyy = dt.strftime("%Y")
            mm = dt.strftime("%m")
            dd = dt.strftime("%d")
            date_compact = dt.strftime("%Y%m%d")
            time_compact = f"{hour:02d}0000"
            path = (
                f"{dataset.ss_base_path}/{yyyy}/{mm}/{dd}/{date_compact}_{time_compact}_{serial}.ss"
            )
            parts.append(f"OUTPUT {rowset_name}")
            parts.append(f'TO SSTREAM "{path}"')
            parts.append(f"    WITH STREAMEXPIRY TimeSpan.FromDays({dataset.stream_expiry_days});")
            parts.append("")

    return "\n".join(parts)


def submit_datagen_job(
    dataset: ScopeDataset,
    adla_account: str,
    au: int = 5,
    priority: int = 1,
    poll_interval: int = 5,
    max_wait: int = 3600,
    http_timeout: int = 30,
    http_retries: int = 3,
) -> str:
    """Submit the datagen SCOPE script to ADLA and wait for completion.

    Returns the job ID.
    """
    # Import here to avoid hard dependency on azure-identity for unit tests
    from dbt.adapters.scope.connections import ScopeConnectionHandle
    from dbt.adapters.scope.credentials import ScopeCredentials

    script = generate_scope_script(dataset)

    log.info(
        "Submitting datagen job: %s (%d days x %d files = %d SS files)",
        dataset.stream_name,
        dataset.days,
        dataset.files_per_day,
        dataset.days * dataset.files_per_day,
    )

    creds = ScopeCredentials(
        database=adla_account,
        schema="datagen",
        adla_account=adla_account,
        http_timeout_seconds=http_timeout,
        http_retries=http_retries,
    )
    handle = ScopeConnectionHandle(creds)
    job = handle.submit_and_wait(
        name=f"datagen-{dataset.stream_name}",
        script=script,
        au=au,
        priority=priority,
        poll_interval=poll_interval,
        max_wait=max_wait,
    )

    log.info("Datagen complete: job_id=%s", job.job_id)
    return job.job_id


def _generate_hours(files_per_day: int) -> list[int]:
    """Generate hour values spread across the day for file timestamps."""
    if files_per_day <= 1:
        return [19]
    return [19 + i for i in range(files_per_day)]


# -- Default test schema matching HelloInsert.script -------------------------

DEFAULT_COLUMNS = [
    ScopeColumn("logical_server_name", "string"),
    ScopeColumn("logical_database_name", "string"),
    ScopeColumn("edition", "string"),
    ScopeColumn("state", "string"),
    ScopeColumn("region_name", "string"),
    ScopeColumn("max_size_bytes", "long"),
    ScopeColumn("PreciseTimeStamp", "DateTime"),
]

DEFAULT_ROWS = [
    ('"server-001"', '"db-alpha"', '"Standard"', '"Online"', '"West US"', "1073741824L", None),
    ('"server-001"', '"db-beta"', '"Premium"', '"Online"', '"West US"', "2147483648L", None),
    ('"server-002"', '"db-gamma"', '"Basic"', '"Online"', '"East US"', "536870912L", None),
    (
        '"server-003"',
        '"db-delta"',
        '"Standard"',
        '"Offline"',
        '"North Europe"',
        "5368709120L",
        None,
    ),
]


def make_default_dataset(
    ss_root: str,
    stream_name: str | None = None,
    start_date: str = "2026-02-01",
    days: int = 60,
    files_per_day: int = 2,
) -> ScopeDataset:
    """Create a dataset using the default HelloInsert schema."""
    if stream_name is None:
        ts = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        stream_name = f"dbt_test_{ts}"

    return ScopeDataset(
        stream_name=stream_name,
        ss_root=ss_root,
        columns=DEFAULT_COLUMNS,
        rows=DEFAULT_ROWS,
        start_date=start_date,
        days=days,
        files_per_day=files_per_day,
    )


# -- Expected data as records ------------------------------------------------


def _parse_scope_value(raw: str, scope_type: str) -> str | int | float:
    """Convert a SCOPE literal string to a Python-native value.

    Examples:
        ``'"server-001"'`` → ``'server-001'`` (strip SCOPE double quotes)
        ``'1073741824L'`` → ``1073741824``     (strip L suffix, cast to int)
    """
    if raw.startswith('"') and raw.endswith('"'):
        return raw[1:-1]
    if scope_type == "long" and raw.endswith("L"):
        return int(raw[:-1])
    if scope_type in ("int", "long"):
        return int(raw)
    if scope_type in ("float", "double"):
        return float(raw)
    return raw


def dataset_to_records(dataset: ScopeDataset) -> list[dict]:
    """Convert a ScopeDataset to a list of row dicts with Python-native types.

    Each record represents one row as it would appear in the Delta table
    after EXTRACT + model transform.  The ``event_year_date`` column is
    computed from the date (``yyyyMMdd`` format).

    Returns one record per ``row x file_per_day x day``.
    """
    records: list[dict] = []
    for dt in dataset.date_range:
        for _serial in range(dataset.files_per_day):
            for row in dataset.rows:
                record: dict = {}
                for i, col in enumerate(dataset.columns):
                    val = row[i]
                    if val is None and col.name == "PreciseTimeStamp":
                        record[col.name] = f"{dt.isoformat()} 19:00:00"
                    else:
                        record[col.name] = _parse_scope_value(str(val), col.scope_type)
                record["event_year_date"] = dt.strftime("%Y%m%d")
                records.append(record)
    return records
