"""Integration test fixtures.

Two-phase datagen simulates a production lifecycle:
  Phase 1: Historical data (3 days) -> full refresh
  Phase 2: New data arrives (2 more days) -> incremental picks it up

All names are descriptive so you can trace SS streams and Delta tables
back to their test scenario.
"""

from __future__ import annotations

import datetime
import json
import logging
import os
import subprocess
import sys
from dataclasses import dataclass
from pathlib import Path

import pytest
from datagen import ScopeDataset, make_default_dataset, submit_datagen_job

PROJECT_DIR = Path(__file__).parent / "dbt_project"

# Unique prefix per session so parallel runs don't collide
_TS = datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%d_%H%M%S")

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
    historical: ScopeDataset  # Phase 1: initial 3 days
    new_data: ScopeDataset  # Phase 2: 2 more days arriving later
    delta_location: str  # Where the Delta table lands


def _build_scenario(label: str, historical_days: int = 3, new_days: int = 2) -> ScenarioConfig:
    """Build a test scenario with datagen datasets."""
    ss_root = _env("SCOPE_SS_TEST_ROOT")
    stream = f"{label}_{_TS}"

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
        start_date="2026-02-04",  # starts after historical
        days=new_days,
        files_per_day=2,
    )

    return ScenarioConfig(
        name=label,
        historical=historical,
        new_data=new_data,
        delta_location=_delta_path(f"{label}_{_TS}"),
    )


@pytest.fixture(scope="session")
def append_scenario() -> ScenarioConfig:
    """Scenario: incremental append (no delete) -- simulates data arriving over time."""
    scenario = _build_scenario("append_no_delete")
    adla = _env("SCOPE_ADLA_ACCOUNT")

    log.info("Generating historical SS files for append scenario")
    submit_datagen_job(scenario.historical, adla_account=adla, au=10)
    return scenario


@pytest.fixture(scope="session")
def delete_insert_scenario() -> ScenarioConfig:
    """Scenario: incremental with delete+insert -- idempotent partition replacement."""
    scenario = _build_scenario("delete_insert")
    adla = _env("SCOPE_ADLA_ACCOUNT")

    log.info("Generating historical SS files for delete+insert scenario")
    submit_datagen_job(scenario.historical, adla_account=adla, au=10)
    return scenario


# -- dbt runner ---------------------------------------------------------------


def _dbt_executable() -> str:
    """Find the dbt executable in the venv."""
    venv_dir = Path(sys.executable).parent
    dbt_exe = venv_dir / "dbt.exe"
    if dbt_exe.exists():
        return str(dbt_exe)
    dbt_exe = venv_dir / "dbt"
    if dbt_exe.exists():
        return str(dbt_exe)
    # Fallback: try dbt.cli.main via python -m
    return f"{sys.executable} -m dbt.cli.main"


def run_dbt(
    args: list[str],
    extra_vars: dict | None = None,
    timeout: int = 3600,
) -> subprocess.CompletedProcess:
    """Run a dbt CLI command against the test project."""
    dbt = _dbt_executable()
    cmd = [
        dbt,
        *args,
        "--project-dir",
        str(PROJECT_DIR),
        "--profiles-dir",
        str(PROJECT_DIR),
    ]
    if extra_vars:
        cmd.extend(["--vars", json.dumps(extra_vars)])

    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        timeout=timeout,
        env=os.environ.copy(),
    )
    print(f"\n--- dbt {' '.join(args)} ---")
    print(result.stdout[-2000:] if len(result.stdout) > 2000 else result.stdout)
    if result.stderr:
        print(result.stderr[-1000:])
    return result


def verify_delta(delta_path: str) -> dict:
    """Verify Delta table via az CLI. Returns parquet_count + partition list."""
    import re as _re

    m = _re.match(r"abfss://([^@]+)@([^.]+)\.dfs\.core\.windows\.net/(.+)", delta_path)
    if not m:
        return {"parquet_count": 0, "partitions": [], "error": f"Bad path: {delta_path}"}

    container, account, prefix = m.group(1), m.group(2), m.group(3)
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
    )
    if result.returncode != 0:
        return {"parquet_count": 0, "partitions": [], "error": result.stderr}

    blobs = json.loads(result.stdout)
    parquet = [b for b in blobs if b.endswith(".parquet")]
    partitions = sorted(
        {pm.group(1) for b in blobs if (pm := _re.search(r"event_year_date[^=]*=(\d+)", b))}
    )
    return {"parquet_count": len(parquet), "partitions": partitions}
