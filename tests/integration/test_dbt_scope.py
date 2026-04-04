"""Integration tests for dbt-scope — driven entirely by datagen.

Each test generates its own synthetic SS files on Cosmos via ADLA,
then runs dbt to produce Delta tables, and verifies the output.

Prerequisites: ADLA account + ADLS + ``az login``.
All env vars come from ``.env`` (see ``.env.example``).
"""

from __future__ import annotations

import logging
import os

import pytest
from conftest import ScenarioConfig, run_dbt, verify_delta
from datagen import submit_datagen_job

log = logging.getLogger(__name__)


def _dbt_vars(scenario: ScenarioConfig, *, with_delete: bool = False) -> dict:
    """Build dbt ``--vars`` dict for a given scenario."""
    return {
        "delta_location": scenario.delta_location,
        "delta_location_with_delete": f"{scenario.delta_location}_del",
        "ss_source_path": scenario.historical.ss_base_path,
        "datagen_start_date": scenario.historical.start_date,
    }


# ---------------------------------------------------------------------------
# Full refresh
# ---------------------------------------------------------------------------


class TestFullRefresh:
    """Full refresh: datagen -> dbt run --full-refresh -> verify Delta."""

    @pytest.mark.timeout(3600)
    def test_full_refresh_creates_delta_partitions(self, append_scenario: ScenarioConfig):
        """Full refresh should create one partition per day of historical data."""
        vars_ = _dbt_vars(append_scenario)

        result = run_dbt(
            ["run", "--full-refresh", "--select", "append_no_delete"],
            extra_vars=vars_,
        )
        assert result.returncode == 0, f"dbt run failed:\n{result.stdout}\n{result.stderr}"

        info = verify_delta(append_scenario.delta_location)
        assert info["parquet_count"] > 0, f"No parquet files found: {info}"
        assert len(info["partitions"]) == append_scenario.historical.days, (
            f"Expected {append_scenario.historical.days} partitions, "
            f"got {len(info['partitions'])}: {info['partitions']}"
        )


# ---------------------------------------------------------------------------
# Incremental append (no delete)
# ---------------------------------------------------------------------------


class TestIncrementalAppend:
    """Incremental append: historical -> full refresh -> new data -> incremental."""

    @pytest.mark.timeout(3600)
    def test_incremental_append_picks_up_new_data(self, append_scenario: ScenarioConfig):
        """After full refresh, generating new SS data and running incremental
        should add new partitions without removing existing ones."""
        vars_ = _dbt_vars(append_scenario)
        adla_account = os.environ.get("SCOPE_ADLA_ACCOUNT", "")

        # Step 1: Full refresh with historical data
        result = run_dbt(
            ["run", "--full-refresh", "--select", "append_no_delete"],
            extra_vars=vars_,
        )
        assert result.returncode == 0, f"Full refresh failed:\n{result.stdout}"

        info_before = verify_delta(append_scenario.delta_location)
        assert info_before["parquet_count"] > 0, "Full refresh produced no parquet files"
        partitions_before = set(info_before["partitions"])

        # Step 2: Generate new SS data (phase 2)
        log.info("Generating new SS data for incremental test")
        submit_datagen_job(append_scenario.new_data, adla_account=adla_account, au=10)

        # Step 3: Incremental run (should pick up only new dates)
        result = run_dbt(
            ["run", "--select", "append_no_delete"],
            extra_vars=vars_,
        )
        assert result.returncode == 0, f"Incremental run failed:\n{result.stdout}"

        info_after = verify_delta(append_scenario.delta_location)
        partitions_after = set(info_after["partitions"])

        # New partitions should be a superset of old
        assert partitions_before.issubset(partitions_after), (
            f"Lost partitions: {partitions_before - partitions_after}"
        )
        total_expected = append_scenario.historical.days + append_scenario.new_data.days
        assert len(partitions_after) == total_expected, (
            f"Expected {total_expected} partitions, got {len(partitions_after)}"
        )


# ---------------------------------------------------------------------------
# Incremental delete+insert (idempotent)
# ---------------------------------------------------------------------------


class TestIncrementalDeleteInsert:
    """Incremental with delete_before_insert: idempotent partition replacement."""

    @pytest.mark.timeout(3600)
    def test_delete_insert_is_idempotent(self, delete_insert_scenario: ScenarioConfig):
        """Running the same batch range twice with delete+insert should not
        create duplicate data — partition count should stay the same."""
        delta_del = f"{delete_insert_scenario.delta_location}_del"

        vars_ = {
            "delta_location": delete_insert_scenario.delta_location,
            "delta_location_with_delete": delta_del,
            "ss_source_path": delete_insert_scenario.historical.ss_base_path,
            "datagen_start_date": delete_insert_scenario.historical.start_date,
        }

        # Step 1: Full refresh
        result = run_dbt(
            ["run", "--full-refresh", "--select", "idempotent_delete_insert"],
            extra_vars=vars_,
        )
        assert result.returncode == 0, f"Full refresh failed:\n{result.stdout}"

        info_first = verify_delta(delta_del)
        assert info_first["parquet_count"] > 0, "Full refresh produced no parquet files"

        # Step 2: Re-run the same range incrementally (delete+insert)
        result = run_dbt(
            ["run", "--select", "idempotent_delete_insert"],
            extra_vars=vars_,
        )
        assert result.returncode == 0, f"Incremental re-run failed:\n{result.stdout}"

        info_second = verify_delta(delta_del)

        # Partition count should remain the same (delete+insert replaced, not appended)
        assert set(info_first["partitions"]) == set(info_second["partitions"]), (
            f"Partitions changed: {info_first['partitions']} vs {info_second['partitions']}"
        )

    @pytest.mark.timeout(3600)
    def test_delete_insert_picks_up_new_data(self, delete_insert_scenario: ScenarioConfig):
        """After initial run, new SS data should be picked up by incremental run."""
        adla_account = os.environ.get("SCOPE_ADLA_ACCOUNT", "")
        delta_del = f"{delete_insert_scenario.delta_location}_del_new"

        vars_ = {
            "delta_location": delete_insert_scenario.delta_location,
            "delta_location_with_delete": delta_del,
            "ss_source_path": delete_insert_scenario.historical.ss_base_path,
            "datagen_start_date": delete_insert_scenario.historical.start_date,
        }

        # Step 1: Full refresh with historical data
        result = run_dbt(
            ["run", "--full-refresh", "--select", "idempotent_delete_insert"],
            extra_vars=vars_,
        )
        assert result.returncode == 0, f"Full refresh failed:\n{result.stdout}"

        info_before = verify_delta(delta_del)
        assert info_before["parquet_count"] > 0, "Full refresh produced no parquet files"

        # Step 2: Generate new SS data
        log.info("Generating new SS data for delete+insert incremental test")
        submit_datagen_job(delete_insert_scenario.new_data, adla_account=adla_account, au=10)

        # Step 3: Incremental run
        result = run_dbt(
            ["run", "--select", "idempotent_delete_insert"],
            extra_vars=vars_,
        )
        assert result.returncode == 0, f"Incremental run failed:\n{result.stdout}"

        info_after = verify_delta(delta_del)
        partitions_after = set(info_after["partitions"])

        total_expected = (
            delete_insert_scenario.historical.days + delete_insert_scenario.new_data.days
        )
        assert len(partitions_after) == total_expected, (
            f"Expected {total_expected} partitions, got {len(partitions_after)}"
        )
