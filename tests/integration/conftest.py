"""Integration test fixtures.

Two-phase datagen simulates a production lifecycle:
  Phase 1: Historical data (5 days) -> full refresh
  Phase 2: New data arrives (2 more days) -> incremental picks it up

All names are descriptive so you can trace SS streams and Delta tables
back to their test scenario.
"""

from __future__ import annotations

import datetime
import json
import logging
import os
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path

import duckdb
import pytest
from azure.identity import AzureCliCredential
from datagen import ScopeDataset, make_default_dataset, submit_datagen_job
from dbt.cli.main import dbtRunner

from dbt.adapters.scope.delta_lake import DuckDbDeltaLakeClient, LockedTokenCredential

PROJECT_DIR = Path(__file__).parent / "dbt_project"
REPO_ROOT = Path(__file__).parent.parent.parent
LOGS_DIR = REPO_ROOT / ".logs"

# Unique prefix per worker so xdist parallel runs don't collide
_WORKER = os.environ.get("PYTEST_XDIST_WORKER", "gw0")
_TS = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%d_%H%M%S")
_PREFIX = f"{_TS}_{_WORKER}"

log = logging.getLogger(__name__)


def _env(name: str) -> str:
    val = os.environ.get(name, "")
    if not val:
        pytest.skip(f"{name} not set -- check .env")
    return val


def _delta_path(table_name: str) -> str:
    storage = _env("SCOPE_STORAGE_ACCOUNT")
    container = _env("SCOPE_CONTAINER")
    base = _env("SCOPE_DELTA_BASE_PATH")
    return f"abfss://{container}@{storage}.dfs.core.windows.net/{base}/{table_name}"


def _delta_client():
    # Integration tests run on dev machines authenticated via ``az login``.
    return DuckDbDeltaLakeClient(credential=LockedTokenCredential(AzureCliCredential()))


# -- Datagen datasets --------------------------------------------------------


@dataclass
class ScenarioConfig:
    """Holds both phases of a test scenario's SS data + Delta paths."""

    name: str
    historical: ScopeDataset  # Phase 1: initial 31 days
    new_data: ScopeDataset  # Phase 2: 2 more days arriving later
    delta_location: str  # Where the Delta table lands


def _build_scenario(label: str, historical_days: int = 5, new_days: int = 2) -> ScenarioConfig:
    """Build a test scenario with datagen datasets."""
    ss_root = _env("SCOPE_SS_TEST_ROOT")
    stream = f"{label}_{_PREFIX}"

    historical = make_default_dataset(
        ss_root=ss_root,
        stream_name=stream,
        start_date="2026-02-01",
        days=historical_days,
        files_per_day=2,
    )
    # new_data starts right after the historical window
    new_start = (date.fromisoformat("2026-02-01") + timedelta(days=historical_days)).isoformat()
    new_data = make_default_dataset(
        ss_root=ss_root,
        stream_name=stream,  # same stream -- new files appear in later dates
        start_date=new_start,
        days=new_days,
        files_per_day=2,
    )

    return ScenarioConfig(
        name=label,
        historical=historical,
        new_data=new_data,
        delta_location=_delta_path(f"{label}_{_PREFIX}"),
    )


@pytest.fixture(scope="session")
def append_scenario() -> ScenarioConfig:
    """Scenario: incremental append (no delete) -- simulates data arriving over time."""
    scenario = _build_scenario("append_no_delete")
    adla = _env("SCOPE_ADLA_ACCOUNT")

    log.info("Generating historical SS files for append scenario")
    submit_datagen_job(scenario.historical, adla_account=adla, au=5)
    return scenario


# -- dbt runner ---------------------------------------------------------------


def _test_log_dir(test_name: str) -> Path:
    """Return the log directory for a specific test, creating it if needed."""
    log_dir = LOGS_DIR / test_name
    log_dir.mkdir(parents=True, exist_ok=True)
    return log_dir


def run_dbt(
    args: list[str],
    extra_vars: dict | None = None,
    test_name: str = "default",
):
    """Run a dbt command using dbtRunner with per-invocation log directory.

    Each invocation gets its own log directory under .logs/<test_name>/
    using dbt's --log-path argument for clean separation.

    Returns the dbtRunner result object with .success and .result attributes.
    """
    log_dir = _test_log_dir(test_name)

    cmd = [
        *args,
        "--project-dir",
        str(PROJECT_DIR),
        "--profiles-dir",
        str(PROJECT_DIR),
        "--log-path",
        str(log_dir),
    ]
    if extra_vars:
        cmd.extend(["--vars", json.dumps(extra_vars)])

    runner = dbtRunner()
    result = runner.invoke(cmd)

    _flush_dbt_logs(log_dir)
    status = "success" if result.success else "FAILED"
    log.info("dbt %s → %s (logs: %s)", " ".join(args), status, log_dir)
    print(f"\n--- dbt {' '.join(args)} [{status}] ---")
    print(f"    Logs: {log_dir}")

    summary_file = log_dir / "result_summary.txt"
    summary_file.write_text(
        f"command: dbt {' '.join(args)}\nsuccess: {result.success}\nresult: {result.result}\n"
    )

    return result


def _flush_dbt_logs(log_dir: Path) -> None:
    """Flush dbt's event logger and fsync log files to disk."""
    try:
        from dbt_common.events.event_manager_client import get_event_manager

        get_event_manager().flush()
    except Exception:
        pass

    log_file = log_dir / "dbt.log"
    if log_file.exists():
        try:
            with open(log_file, "a") as f:
                os.fsync(f.fileno())
        except OSError:
            pass


# -- Delta verification -------------------------------------------------------


def count_delta_log_files(delta_path: str) -> int:
    """Count JSON transaction-log files in a Delta table's ``_delta_log/`` directory."""
    return _delta_client().count_delta_log_files(delta_path)


def verify_delta(delta_path: str) -> dict:
    """Verify Delta table layout via the shared Delta Lake client."""
    return _delta_client().describe_table_files(delta_path)


def query_delta_with_duckdb(query: str) -> list[tuple]:
    """Execute a read-only DuckDB query with Delta + Azure extensions preloaded."""
    return _delta_client().fetchall(query)


def verify_delta_with_duckdb(
    delta_path: str,
    expected_manifest: dict[str, int] | None = None,
    expected_total_rows: int | None = None,
) -> dict:
    """Validate Delta table contents using DuckDB with delta + azure extensions.

    Args:
        delta_path: abfss:// path to the Delta table.
        expected_manifest: Optional dict of partition_value → expected row count.
        expected_total_rows: Optional total expected row count.

    Returns:
        dict with keys: total_rows, partition_counts, errors.
    """
    log.info("Verifying Delta table with DuckDB at %s", delta_path)
    delta_client = _delta_client()
    result_info: dict = {"total_rows": 0, "partition_counts": {}, "errors": []}

    try:
        # Total row count
        total = delta_client.get_total_row_count(delta_path)
        result_info["total_rows"] = total

        if expected_total_rows is not None and total != expected_total_rows:
            result_info["errors"].append(
                f"Total rows mismatch: expected {expected_total_rows}, got {total}"
            )

        # Per-partition row counts
        try:
            partition_counts = delta_client.get_partition_counts(delta_path, "event_year_date")
            result_info["partition_counts"] = partition_counts

            if expected_manifest:
                for part_val, expected_count in expected_manifest.items():
                    actual = partition_counts.get(part_val, 0)
                    if actual != expected_count:
                        result_info["errors"].append(
                            f"Partition {part_val}: expected {expected_count} rows, got {actual}"
                        )
                # Check for unexpected partitions (ignore None from null partition values)
                actual_keys = {k for k in partition_counts if k not in ("None", "null", "")}
                unexpected = actual_keys - set(expected_manifest.keys())
                if unexpected:
                    result_info["errors"].append(f"Unexpected partitions: {sorted(unexpected)}")
        except duckdb.Error as e:
            result_info["errors"].append(f"Partition query failed: {e}")

    except duckdb.Error as e:
        err_msg = str(e)
        result_info["errors"].append(f"Delta scan failed: {e}")
        if "No files in log segment" in err_msg or "does not exist" in err_msg:
            log.debug("Delta table does not exist yet at %s (expected on first run)", delta_path)
        else:
            log.error("DuckDB Delta scan failed: %s", e)

    log.info(
        "DuckDB verify result: total_rows=%d, partitions=%d, errors=%d",
        result_info["total_rows"],
        len(result_info["partition_counts"]),
        len(result_info["errors"]),
    )
    if result_info["errors"]:
        for err in result_info["errors"]:
            if "No files in log segment" in err or "does not exist" in err:
                log.debug("DuckDB verify (expected): %s", err)
            else:
                log.warning("DuckDB verify error: %s", err)
    return result_info


# -- Watermark + sources checkpoint verification ------------------------------


def read_watermark(delta_path: str):
    """Read the watermark checkpoint for a Delta table."""
    from dbt.adapters.scope.checkpoint import CheckpointManager

    return CheckpointManager().read_watermark(delta_path)


def list_source_files(delta_path: str) -> list[str]:
    """List files in ``_checkpoint/sources/``."""
    from dbt.adapters.scope.checkpoint import CheckpointManager

    return CheckpointManager().list_source_files(delta_path)


def read_batch_source(delta_path: str, batch_id: int) -> list[dict]:
    """Read a batch JSONL file from ``_checkpoint/sources/{batch_id}``."""
    from dbt.adapters.scope.checkpoint import CheckpointManager

    return CheckpointManager().read_batch_source(delta_path, batch_id)
