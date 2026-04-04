"""Integration test fixtures.

Two-phase datagen simulates a production lifecycle:
  Phase 1: Historical data (31 days) -> full refresh
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
from pathlib import Path

import duckdb
import pytest
from azure.identity import AzureCliCredential
from datagen import ScopeDataset, make_default_dataset, submit_datagen_job
from dbt.cli.main import dbtRunner

from dbt.adapters.scope._file_lock import AZ_CLI_TOKEN_LOCK, FileLock

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


# -- Datagen datasets --------------------------------------------------------


@dataclass
class ScenarioConfig:
    """Holds both phases of a test scenario's SS data + Delta paths."""

    name: str
    historical: ScopeDataset  # Phase 1: initial 31 days
    new_data: ScopeDataset  # Phase 2: 2 more days arriving later
    delta_location: str  # Where the Delta table lands


def _build_scenario(label: str, historical_days: int = 31, new_days: int = 2) -> ScenarioConfig:
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
    new_data = make_default_dataset(
        ss_root=ss_root,
        stream_name=stream,  # same stream -- new files appear in later dates
        start_date="2026-03-04",  # starts after historical
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


@pytest.fixture(scope="session")
def delete_insert_scenario() -> ScenarioConfig:
    """Scenario: incremental with delete+insert -- idempotent partition replacement."""
    scenario = _build_scenario("delete_insert")
    adla = _env("SCOPE_ADLA_ACCOUNT")

    log.info("Generating historical SS files for delete+insert scenario")
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


def verify_delta(delta_path: str) -> dict:
    """Verify Delta table via az CLI. Returns parquet_count + partition list."""
    import re as _re

    log.info("Verifying Delta table via az CLI at %s", delta_path)

    m = _re.match(r"abfss://([^@]+)@([^.]+)\.dfs\.core\.windows\.net/(.+)", delta_path)
    if not m:
        log.warning("Bad Delta path format: %s", delta_path)
        return {"parquet_count": 0, "partitions": [], "error": f"Bad path: {delta_path}"}

    container, account, prefix = m.group(1), m.group(2), m.group(3)

    import subprocess

    result = subprocess.run(
        [
            "az",
            "storage",
            "blob",
            "list",
            "--container-name",
            container,
            "--prefix",
            f"{prefix}/",
            "--account-name",
            account,
            "--auth-mode",
            "login",
            "--query",
            "[?properties.contentLength>`0`].name",
            "-o",
            "json",
        ],
        capture_output=True,
        text=True,
        timeout=60,
        shell=True,
    )
    if result.returncode != 0:
        log.warning("az storage blob list failed: %s", result.stderr)
        return {"parquet_count": 0, "partitions": [], "error": result.stderr}

    blobs = json.loads(result.stdout)
    parquet = [b for b in blobs if b.endswith(".parquet")]
    partitions = sorted(
        {pm.group(1) for b in blobs if (pm := _re.search(r"event_year_date[^=]*=(\d+)", b))}
    )
    log.info("Delta verify result: %d parquet files, %d partitions", len(parquet), len(partitions))
    return {"parquet_count": len(parquet), "partitions": partitions}


def _get_storage_token() -> str:
    """Acquire an Azure Storage token using the file-locked credential."""
    cred = AzureCliCredential()
    with FileLock(AZ_CLI_TOKEN_LOCK):
        token = cred.get_token("https://storage.azure.com/.default")
    return token.token


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
    conn = duckdb.connect()
    conn.execute("INSTALL delta;")
    conn.execute("LOAD delta;")
    conn.execute("INSTALL azure;")
    conn.execute("LOAD azure;")

    token = _get_storage_token()
    conn.execute(f"CREATE SECRET az1 (TYPE AZURE, PROVIDER ACCESS_TOKEN, ACCESS_TOKEN '{token}');")
    result_info: dict = {"total_rows": 0, "partition_counts": {}, "errors": []}

    try:
        # Total row count
        row = conn.execute(f"SELECT COUNT(*) AS cnt FROM delta_scan('{delta_path}')").fetchone()
        total = row[0] if row else 0
        result_info["total_rows"] = total

        if expected_total_rows is not None and total != expected_total_rows:
            result_info["errors"].append(
                f"Total rows mismatch: expected {expected_total_rows}, got {total}"
            )

        # Per-partition row counts
        try:
            rows = conn.execute(
                f"SELECT event_year_date, COUNT(*) AS cnt "
                f"FROM delta_scan('{delta_path}') "
                f"GROUP BY event_year_date ORDER BY event_year_date"
            ).fetchall()
            partition_counts = {str(r[0]): r[1] for r in rows}
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
        result_info["errors"].append(f"Delta scan failed: {e}")
        log.error("DuckDB Delta scan failed: %s", e)
    finally:
        conn.close()

    log.info(
        "DuckDB verify result: total_rows=%d, partitions=%d, errors=%d",
        result_info["total_rows"],
        len(result_info["partition_counts"]),
        len(result_info["errors"]),
    )
    if result_info["errors"]:
        for err in result_info["errors"]:
            log.warning("DuckDB verify error: %s", err)
    return result_info
