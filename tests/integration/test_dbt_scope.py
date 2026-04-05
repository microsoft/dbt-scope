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

import duckdb
import pytest
from conftest import ScenarioConfig, run_dbt, verify_delta_with_duckdb
from datagen import dataset_to_records, submit_datagen_job

log = logging.getLogger(__name__)


def _dbt_vars(scenario: ScenarioConfig, *, with_delete: bool = False) -> dict:
    """Build dbt ``--vars`` dict for a given scenario."""
    return {
        "delta_location": scenario.delta_location,
        "delta_location_with_delete": f"{scenario.delta_location}_del",
        "delta_location_filtered": f"{scenario.delta_location}_filtered",
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

        log.info(
            "Starting full refresh test: delta=%s, days=%d",
            append_scenario.delta_location,
            append_scenario.historical.days,
        )

        result = run_dbt(
            ["run", "--full-refresh", "--select", "append_no_delete"],
            extra_vars=vars_,
            test_name=test_name,
        )
        assert result.success, f"dbt run failed: {result.result}"

        log.info("Verifying Delta table output")
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
        log.info(
            "Full refresh test passed: %d rows, %d partitions",
            duckdb_info["total_rows"],
            len(non_null_partitions),
        )


# ---------------------------------------------------------------------------
# Incremental append (no delete)
# ---------------------------------------------------------------------------


class TestIncrementalAppend:
    """Incremental append: first run creates table, new data → incremental picks it up.

    No ``--full-refresh`` or ``--event-time-start/end`` — the adapter auto-detects
    the high watermark from Delta and only processes new batches.
    """

    @pytest.mark.timeout(3600)
    def test_incremental_append_picks_up_new_data(
        self, append_scenario: ScenarioConfig, request: pytest.FixtureRequest
    ):
        """After initial run, generating new SS data and running again
        should add new partitions without removing existing ones.

        The adapter queries Delta for MAX(partition_col) and skips
        already-processed batches automatically.
        """
        vars_ = _dbt_vars(append_scenario)
        adla_account = os.environ.get("SCOPE_ADLA_ACCOUNT", "")
        test_name = _test_id(request)

        # Step 1: Initial run — table doesn't exist, so dbt does full refresh
        log.info("Step 1: Initial run (auto full-refresh on new table)")
        result = run_dbt(
            ["run", "--select", "append_no_delete"],
            extra_vars=vars_,
            test_name=f"{test_name}_initial",
        )
        assert result.success, f"Initial run failed: {result.result}"

        before_info = verify_delta_with_duckdb(append_scenario.delta_location)
        assert before_info["total_rows"] > 0, "Initial run produced no rows"
        log.info("After initial run: %d rows", before_info["total_rows"])

        # Step 2: Generate new SS data (phase 2)
        log.info("Step 2: Generating new SS data for incremental test")
        submit_datagen_job(append_scenario.new_data, adla_account=adla_account, au=5)

        # Step 3: Incremental run — no --event-time-start/end needed
        # The adapter auto-detects the high watermark from Delta and skips
        # already-processed batches, only processing new data.
        log.info("Step 3: Running incremental (auto high-watermark detection)")
        result = run_dbt(
            ["run", "--select", "append_no_delete"],
            extra_vars=vars_,
            test_name=f"{test_name}_incremental",
        )
        assert result.success, f"Incremental run failed: {result.result}"

        # Verify new data rows are present
        log.info("Verifying incremental results")
        after_info = verify_delta_with_duckdb(append_scenario.delta_location)
        assert after_info["total_rows"] > before_info["total_rows"], (
            f"Incremental should add rows: before={before_info['total_rows']}, "
            f"after={after_info['total_rows']}"
        )
        expected_new = append_scenario.new_data.total_expected_rows
        actual_new = after_info["total_rows"] - before_info["total_rows"]
        assert actual_new >= expected_new, (
            f"Expected at least {expected_new} new rows, got {actual_new}"
        )
        log.info(
            "Incremental append test passed: before=%d, after=%d, new=%d",
            before_info["total_rows"],
            after_info["total_rows"],
            actual_new,
        )


# ---------------------------------------------------------------------------
# Incremental delete+insert (idempotent)
# ---------------------------------------------------------------------------


class TestIncrementalDeleteInsert:
    """Incremental with delete_before_insert: idempotent partition replacement.

    No ``--full-refresh`` or ``--event-time-start/end`` — the adapter auto-detects
    the high watermark from Delta.
    """

    @pytest.mark.timeout(3600)
    def test_delete_insert_is_idempotent(
        self, delete_insert_scenario: ScenarioConfig, request: pytest.FixtureRequest
    ):
        """Running the same data twice should not create duplicate data —
        the high-watermark skip ensures the second run is a no-op for
        already-processed batches."""
        delta_del = f"{delete_insert_scenario.delta_location}_del"
        test_name = _test_id(request)

        vars_ = {
            "delta_location": delete_insert_scenario.delta_location,
            "delta_location_with_delete": delta_del,
            "ss_source_path": delete_insert_scenario.historical.ss_base_path,
            "datagen_start_date": delete_insert_scenario.historical.start_date,
        }

        # Step 1: Initial run — table doesn't exist, dbt does full refresh
        log.info("Step 1: Initial run for idempotent delete+insert test")
        result = run_dbt(
            ["run", "--select", "idempotent_delete_insert"],
            extra_vars=vars_,
            test_name=f"{test_name}_initial",
        )
        assert result.success, f"Initial run failed: {result.result}"

        first_info = verify_delta_with_duckdb(
            delta_del,
            expected_total_rows=delete_insert_scenario.historical.total_expected_rows,
        )
        assert first_info["total_rows"] > 0, "Initial run produced no rows"
        log.info("After initial run: %d rows", first_info["total_rows"])

        # Step 2: Re-run incrementally — high-watermark should skip all batches
        log.info("Step 2: Re-running incrementally to test idempotency")
        result = run_dbt(
            ["run", "--select", "idempotent_delete_insert"],
            extra_vars=vars_,
            test_name=f"{test_name}_incremental",
        )
        assert result.success, f"Incremental re-run failed: {result.result}"

        second_info = verify_delta_with_duckdb(delta_del)
        # Row count should stay the same — high-watermark skip prevents reprocessing
        assert second_info["total_rows"] == first_info["total_rows"], (
            f"Row count changed after idempotent re-run: "
            f"first={first_info['total_rows']}, second={second_info['total_rows']}"
        )
        log.info(
            "Idempotent test passed: first=%d, second=%d (equal ✓)",
            first_info["total_rows"],
            second_info["total_rows"],
        )

    @pytest.mark.timeout(3600)
    def test_delete_insert_picks_up_new_data(
        self, delete_insert_scenario: ScenarioConfig, request: pytest.FixtureRequest
    ):
        """After initial run, new SS data should be picked up by incremental run.

        No ``--event-time-start/end`` — the adapter queries Delta for the
        high watermark and only processes batches after it.
        """
        adla_account = os.environ.get("SCOPE_ADLA_ACCOUNT", "")
        delta_del = f"{delete_insert_scenario.delta_location}_del_new"
        test_name = _test_id(request)

        vars_ = {
            "delta_location": delete_insert_scenario.delta_location,
            "delta_location_with_delete": delta_del,
            "ss_source_path": delete_insert_scenario.historical.ss_base_path,
            "datagen_start_date": delete_insert_scenario.historical.start_date,
        }

        # Step 1: Initial run — table doesn't exist, dbt does full refresh
        log.info("Step 1: Initial run for delete+insert new data test")
        result = run_dbt(
            ["run", "--select", "idempotent_delete_insert"],
            extra_vars=vars_,
            test_name=f"{test_name}_initial",
        )
        assert result.success, f"Initial run failed: {result.result}"

        before_info = verify_delta_with_duckdb(delta_del)
        assert before_info["total_rows"] > 0, "Initial run produced no rows"
        log.info("After initial run: %d rows", before_info["total_rows"])

        # Step 2: Generate new SS data
        log.info("Step 2: Generating new SS data for delete+insert incremental test")
        submit_datagen_job(delete_insert_scenario.new_data, adla_account=adla_account, au=5)

        # Step 3: Incremental run — auto high-watermark detection
        log.info("Step 3: Running incremental (auto high-watermark detection)")
        result = run_dbt(
            ["run", "--select", "idempotent_delete_insert"],
            extra_vars=vars_,
            test_name=f"{test_name}_incremental",
        )
        assert result.success, f"Incremental run failed: {result.result}"

        # DuckDB: verify new rows were added
        log.info("Verifying delete+insert incremental results")
        after_info = verify_delta_with_duckdb(delta_del)
        assert after_info["total_rows"] > before_info["total_rows"], (
            f"Incremental should add rows: before={before_info['total_rows']}, "
            f"after={after_info['total_rows']}"
        )
        log.info(
            "Delete+insert new data test passed: before=%d, after=%d",
            before_info["total_rows"],
            after_info["total_rows"],
        )


# ---------------------------------------------------------------------------
# Filtered edition (WHERE clause in model SQL)
# ---------------------------------------------------------------------------


class TestFilteredEdition:
    """Filtered model: only 'Standard' edition rows should land in Delta.

    Uses the same SS source data as the append scenario but the model SQL
    contains ``WHERE edition == "Standard"``, so the adapter must merge
    the date predicate with AND rather than adding a second WHERE.
    """

    @pytest.mark.timeout(3600)
    def test_filtered_full_refresh_only_standard_rows(
        self, append_scenario: ScenarioConfig, request: pytest.FixtureRequest
    ):
        """Full refresh with edition filter should produce only 'Standard' rows."""
        vars_ = _dbt_vars(append_scenario)
        test_name = _test_id(request)
        delta_filtered = f"{append_scenario.delta_location}_filtered"

        log.info("Starting filtered edition test: delta=%s", delta_filtered)

        result = run_dbt(
            ["run", "--full-refresh", "--select", "filtered_edition"],
            extra_vars=vars_,
            test_name=test_name,
        )
        assert result.success, f"dbt run failed: {result.result}"

        # Build expected records filtered to edition == "Standard"
        log.info("Building expected records for filtered assertion")
        all_records = dataset_to_records(append_scenario.historical)
        expected_standard = [r for r in all_records if r.get("edition") == "Standard"]

        # Query Delta and compare
        delta_info = verify_delta_with_duckdb(delta_filtered)
        assert not delta_info["errors"], "DuckDB validation errors:\n" + "\n".join(
            delta_info["errors"]
        )

        # Row count: only Standard rows
        assert delta_info["total_rows"] == len(expected_standard), (
            f"Expected {len(expected_standard)} Standard rows, got {delta_info['total_rows']}"
        )

        # Verify all rows in Delta have edition == "Standard"
        from conftest import _get_storage_token

        conn = duckdb.connect()
        try:
            conn.execute("INSTALL delta; LOAD delta; INSTALL azure; LOAD azure;")
            token = _get_storage_token()
            conn.execute(
                f"CREATE SECRET az1 (TYPE AZURE, PROVIDER ACCESS_TOKEN, ACCESS_TOKEN '{token}');"
            )

            # No non-Standard editions should exist
            non_standard = conn.execute(
                f"SELECT DISTINCT edition FROM delta_scan('{delta_filtered}') "
                f"WHERE edition != 'Standard'"
            ).fetchall()
            assert non_standard == [], (
                f"Found non-Standard editions in filtered Delta: {non_standard}"
            )

            # Per-partition row counts should match expected
            expected_per_partition: dict[str, int] = {}
            for r in expected_standard:
                p = str(r["event_year_date"])
                expected_per_partition[p] = expected_per_partition.get(p, 0) + 1

            actual_partitions = conn.execute(
                f"SELECT event_year_date, COUNT(*) AS cnt "
                f"FROM delta_scan('{delta_filtered}') "
                f"GROUP BY event_year_date ORDER BY event_year_date"
            ).fetchall()
            actual_per_partition = {str(r[0]): r[1] for r in actual_partitions}

            assert actual_per_partition == expected_per_partition, (
                f"Partition counts mismatch:\n"
                f"  expected: {expected_per_partition}\n"
                f"  actual:   {actual_per_partition}"
            )
            log.info(
                "Filtered edition test passed: %d Standard rows across %d partitions",
                len(expected_standard),
                len(actual_per_partition),
            )
        finally:
            conn.close()
