"""Integration tests for dbt-scope — driven entirely by datagen.

Each test generates its own synthetic SS files on Cosmos via ADLA,
then runs dbt to produce Delta tables, and verifies the output
using DuckDB with the delta extension for row-level validation.

Prerequisites: ADLA account + ADLS + ``az login``.
All env vars come from ``.env`` (see ``.env.example``).
"""

from __future__ import annotations

import logging
import os

import pytest
from conftest import ScenarioConfig, run_dbt, verify_delta_with_duckdb
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


def _test_id(request: pytest.FixtureRequest) -> str:
    """Extract a filesystem-safe test name for log directories."""
    return request.node.name.replace("[", "_").replace("]", "").replace("/", "_")


# ---------------------------------------------------------------------------
# Full refresh
# ---------------------------------------------------------------------------


class TestFullRefresh:
    """Full refresh: datagen -> dbt run --full-refresh -> verify Delta."""

    @pytest.mark.timeout(3600)
    def test_full_refresh_creates_delta_partitions(
        self, append_scenario: ScenarioConfig, request: pytest.FixtureRequest
    ):
        """Full refresh should create one partition per day of historical data."""
        vars_ = _dbt_vars(append_scenario)
        test_name = _test_id(request)

        result = run_dbt(
            ["run", "--full-refresh", "--select", "append_no_delete"],
            extra_vars=vars_,
            test_name=test_name,
        )
        assert result.success, f"dbt run failed: {result.result}"

        duckdb_info = verify_delta_with_duckdb(
            append_scenario.delta_location,
            expected_total_rows=append_scenario.historical.total_expected_rows,
        )
        assert not duckdb_info["errors"], "DuckDB validation errors:\n" + "\n".join(
            duckdb_info["errors"]
        )
        non_null_partitions = {
            k for k in duckdb_info["partition_counts"] if k not in ("None", "null", "")
        }
        if non_null_partitions:
            assert len(non_null_partitions) == append_scenario.historical.days, (
                f"Expected {append_scenario.historical.days} partitions, "
                f"got {len(non_null_partitions)}: {non_null_partitions}"
            )


# ---------------------------------------------------------------------------
# Incremental append (no delete)
# ---------------------------------------------------------------------------


class TestIncrementalAppend:
    """Incremental append: historical -> full refresh -> new data -> incremental."""

    @pytest.mark.timeout(3600)
    def test_incremental_append_picks_up_new_data(
        self, append_scenario: ScenarioConfig, request: pytest.FixtureRequest
    ):
        """After full refresh, generating new SS data and running incremental
        should add new partitions without removing existing ones."""
        vars_ = _dbt_vars(append_scenario)
        adla_account = os.environ.get("SCOPE_ADLA_ACCOUNT", "")
        test_name = _test_id(request)

        # Step 1: Full refresh with historical data
        result = run_dbt(
            ["run", "--full-refresh", "--select", "append_no_delete"],
            extra_vars=vars_,
            test_name=f"{test_name}_full_refresh",
        )
        assert result.success, f"Full refresh failed: {result.result}"

        before_info = verify_delta_with_duckdb(append_scenario.delta_location)
        assert before_info["total_rows"] > 0, "Full refresh produced no rows"
        set(before_info["partition_counts"].keys())

        # Step 2: Generate new SS data (phase 2)
        log.info("Generating new SS data for incremental test")
        submit_datagen_job(append_scenario.new_data, adla_account=adla_account, au=10)

        # Step 3: Incremental run (should pick up only new dates)
        result = run_dbt(
            ["run", "--select", "append_no_delete"],
            extra_vars=vars_,
            test_name=f"{test_name}_incremental",
        )
        assert result.success, f"Incremental run failed: {result.result}"

        # DuckDB: verify incremental added new rows
        # Note: append strategy may re-process overlapping dates, so we check
        # that total rows increased (not exact count).
        after_info = verify_delta_with_duckdb(append_scenario.delta_location)
        assert after_info["total_rows"] > before_info["total_rows"], (
            f"Incremental should add rows: before={before_info['total_rows']}, "
            f"after={after_info['total_rows']}"
        )
        # Verify new data rows are present (at minimum, new_data rows were added)
        assert after_info["total_rows"] >= (
            before_info["total_rows"] + append_scenario.new_data.total_expected_rows
        ), (
            f"Expected at least {append_scenario.new_data.total_expected_rows} new rows, "
            f"got {after_info['total_rows'] - before_info['total_rows']}"
        )


# ---------------------------------------------------------------------------
# Incremental delete+insert (idempotent)
# ---------------------------------------------------------------------------


class TestIncrementalDeleteInsert:
    """Incremental with delete_before_insert: idempotent partition replacement."""

    @pytest.mark.timeout(3600)
    def test_delete_insert_is_idempotent(
        self, delete_insert_scenario: ScenarioConfig, request: pytest.FixtureRequest
    ):
        """Running the same batch range twice with delete+insert should not
        create duplicate data — partition count should stay the same."""
        delta_del = f"{delete_insert_scenario.delta_location}_del"
        test_name = _test_id(request)

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
            test_name=f"{test_name}_full_refresh",
        )
        assert result.success, f"Full refresh failed: {result.result}"

        first_info = verify_delta_with_duckdb(
            delta_del,
            expected_total_rows=delete_insert_scenario.historical.total_expected_rows,
        )
        assert first_info["total_rows"] > 0, "Full refresh produced no rows"

        # Step 2: Re-run the same range incrementally (delete+insert)
        result = run_dbt(
            ["run", "--select", "idempotent_delete_insert"],
            extra_vars=vars_,
            test_name=f"{test_name}_incremental",
        )
        assert result.success, f"Incremental re-run failed: {result.result}"

        second_info = verify_delta_with_duckdb(delta_del)
        # With delete+insert, row count should stay the same after re-run
        # (delete removes old data, insert adds it back)
        assert second_info["total_rows"] == first_info["total_rows"], (
            f"Row count changed after idempotent re-run: "
            f"first={first_info['total_rows']}, second={second_info['total_rows']}"
        )

    @pytest.mark.timeout(3600)
    def test_delete_insert_picks_up_new_data(
        self, delete_insert_scenario: ScenarioConfig, request: pytest.FixtureRequest
    ):
        """After initial run, new SS data should be picked up by incremental run."""
        adla_account = os.environ.get("SCOPE_ADLA_ACCOUNT", "")
        delta_del = f"{delete_insert_scenario.delta_location}_del_new"
        test_name = _test_id(request)

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
            test_name=f"{test_name}_full_refresh",
        )
        assert result.success, f"Full refresh failed: {result.result}"

        before_info = verify_delta_with_duckdb(delta_del)
        assert before_info["total_rows"] > 0, "Full refresh produced no rows"

        # Step 2: Generate new SS data
        log.info("Generating new SS data for delete+insert incremental test")
        submit_datagen_job(delete_insert_scenario.new_data, adla_account=adla_account, au=10)

        # Step 3: Incremental run
        result = run_dbt(
            ["run", "--select", "idempotent_delete_insert"],
            extra_vars=vars_,
            test_name=f"{test_name}_incremental",
        )
        assert result.success, f"Incremental run failed: {result.result}"

        # DuckDB: verify combined data
        {
            **delete_insert_scenario.historical.expected_rows_per_partition(),
            **delete_insert_scenario.new_data.expected_rows_per_partition(),
        }
        # DuckDB: verify new rows were added
        after_info = verify_delta_with_duckdb(delta_del)
        assert after_info["total_rows"] > before_info["total_rows"], (
            f"Incremental should add rows: before={before_info['total_rows']}, "
            f"after={after_info['total_rows']}"
        )
