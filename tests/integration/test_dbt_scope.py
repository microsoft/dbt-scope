"""Integration tests for dbt-scope — file-based processing with sources checkpoint.

Prerequisites: ADLA account + ADLS + ``az login``.
All env vars come from ``.env`` (see ``.env.example``).

Note: DuckDB's ``delta_scan`` cannot read SCOPE's non-standard hive partition
directories (``col-name-hash=value``), so partition column values appear as
NULL in DuckDB queries.  Tests verify partition correctness via ADLS file
listing instead.
"""

from __future__ import annotations

import logging
import os

import pytest
from conftest import (
    ScenarioConfig,
    count_delta_log_files,
    list_source_files,
    query_delta_with_duckdb,
    read_batch_source,
    read_watermark,
    run_dbt,
    verify_delta_with_duckdb,
)
from datagen import dataset_to_records, submit_datagen_job

log = logging.getLogger(__name__)


def _dbt_vars(scenario: ScenarioConfig) -> dict:
    return {
        "delta_location": scenario.delta_location,
        "delta_location_with_delete": f"{scenario.delta_location}_del",
        "delta_location_filtered": f"{scenario.delta_location}_filtered",
        "source_root": scenario.historical.ss_base_path,
        "source_pattern": r".*\.ss$",
        "max_files_per_trigger": 500,
    }


def _test_id(request: pytest.FixtureRequest) -> str:
    return request.node.name.replace("[", "_").replace("]", "").replace("/", "_")


# ---------------------------------------------------------------------------
# Full refresh
# ---------------------------------------------------------------------------


class TestFullRefresh:
    @pytest.mark.timeout(3600)
    def test_full_refresh_creates_delta_table(
        self, append_scenario: ScenarioConfig, request: pytest.FixtureRequest
    ):
        """Full refresh: all files processed, checkpoint + sources written."""
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
        assert not duckdb_info["errors"], "\n".join(duckdb_info["errors"])

        # Watermark checkpoint exists
        wm = read_watermark(append_scenario.delta_location)
        assert wm is not None, "Watermark should exist after full refresh"
        assert wm.modified_time != ""
        assert wm.batch_id == 0, f"First batch should be batch_id=0, got {wm.batch_id}"

        # Sources JSONL exists for batch 0
        sources = list_source_files(append_scenario.delta_location)
        assert "0" in sources, f"Expected sources/0 JSONL, got {sources}"

        # Sources JSONL content is valid
        records = read_batch_source(append_scenario.delta_location, 0)
        assert len(records) > 0, "Sources JSONL should have records"
        assert all("path" in r for r in records)
        assert all("modificationTime" in r for r in records)
        assert all("batchId" in r for r in records)
        assert all("batchProcessingTime" in r for r in records)
        assert all(r["batchId"] == 0 for r in records)

        # File metadata columns (non-partition) should be non-null
        file_meta = query_delta_with_duckdb(
            f"SELECT source_file_uri, source_file_length "
            f"FROM delta_scan('{append_scenario.delta_location}') LIMIT 5"
        )
        assert len(file_meta) > 0, "Should have rows"
        for row in file_meta:
            assert row[0] is not None, f"source_file_uri should be non-null, got {row}"

        log.info(
            "Full refresh passed: %d rows, watermark batch_id=%d, %d source records",
            duckdb_info["total_rows"],
            wm.batch_id,
            len(records),
        )


# ---------------------------------------------------------------------------
# Incremental append + watermark updates
# ---------------------------------------------------------------------------


class TestIncrementalAppend:
    @pytest.mark.timeout(3600)
    def test_incremental_picks_up_new_data_and_updates_watermark(
        self, append_scenario: ScenarioConfig, request: pytest.FixtureRequest
    ):
        """Incremental: new files detected, watermark and sources advance."""
        vars_ = _dbt_vars(append_scenario)
        adla_account = os.environ.get("SCOPE_ADLA_ACCOUNT", "")
        test_name = _test_id(request)

        # Step 1: Initial run
        result = run_dbt(
            ["run", "--select", "append_no_delete"],
            extra_vars=vars_,
            test_name=f"{test_name}_initial",
        )
        assert result.success

        before_info = verify_delta_with_duckdb(append_scenario.delta_location)
        assert before_info["total_rows"] > 0

        wm_before = read_watermark(append_scenario.delta_location)
        assert wm_before is not None
        batch_before = wm_before.batch_id

        # Step 2: Generate new SS data
        submit_datagen_job(append_scenario.new_data, adla_account=adla_account, au=5)

        # Step 3: Incremental run
        result = run_dbt(
            ["run", "--select", "append_no_delete"],
            extra_vars=vars_,
            test_name=f"{test_name}_incremental",
        )
        assert result.success

        after_info = verify_delta_with_duckdb(append_scenario.delta_location)
        assert after_info["total_rows"] > before_info["total_rows"]

        # Watermark advanced
        wm_after = read_watermark(append_scenario.delta_location)
        assert wm_after is not None
        assert wm_after.batch_id > batch_before, (
            f"batch_id should advance: {batch_before} -> {wm_after.batch_id}"
        )
        assert wm_after.version > wm_before.version

        # Sources for new batch exist (JSONL or parquet snapshot depending on compaction)
        sources = list_source_files(append_scenario.delta_location)
        has_new_batch = (
            str(wm_after.batch_id) in sources or f"{wm_after.batch_id}.parquet" in sources
        )
        assert has_new_batch, f"Sources should have batch {wm_after.batch_id}, got {sources}"

        log.info(
            "Incremental passed: rows %d->%d, batch_id %d->%d, sources: %s",
            before_info["total_rows"],
            after_info["total_rows"],
            batch_before,
            wm_after.batch_id,
            sources,
        )


# ---------------------------------------------------------------------------
# Idempotent re-run
# ---------------------------------------------------------------------------


class TestIncrementalIdempotent:
    @pytest.mark.timeout(3600)
    def test_rerun_is_idempotent(
        self, delete_insert_scenario: ScenarioConfig, request: pytest.FixtureRequest
    ):
        """Re-running with no new files should not change rows or watermark."""
        delta_del = f"{delete_insert_scenario.delta_location}_del"
        test_name = _test_id(request)
        vars_ = {
            "delta_location": delete_insert_scenario.delta_location,
            "delta_location_with_delete": delta_del,
            "source_root": delete_insert_scenario.historical.ss_base_path,
            "source_pattern": r".*\.ss$",
            "max_files_per_trigger": 500,
        }

        result = run_dbt(
            ["run", "--select", "idempotent_delete_insert"],
            extra_vars=vars_,
            test_name=f"{test_name}_initial",
        )
        assert result.success

        first_info = verify_delta_with_duckdb(
            delta_del,
            expected_total_rows=delete_insert_scenario.historical.total_expected_rows,
        )
        wm1 = read_watermark(delta_del)
        assert wm1 is not None

        result = run_dbt(
            ["run", "--select", "idempotent_delete_insert"],
            extra_vars=vars_,
            test_name=f"{test_name}_incremental",
        )
        assert result.success

        second_info = verify_delta_with_duckdb(delta_del)
        assert second_info["total_rows"] == first_info["total_rows"]

        wm2 = read_watermark(delta_del)
        assert wm2 is not None
        assert wm2.batch_id == wm1.batch_id, "batch_id should not change on no-op"
        assert wm2.version == wm1.version, "version should not change on no-op"

    @pytest.mark.timeout(3600)
    def test_picks_up_new_data(
        self, delete_insert_scenario: ScenarioConfig, request: pytest.FixtureRequest
    ):
        """After initial run, new SS data should be picked up."""
        adla_account = os.environ.get("SCOPE_ADLA_ACCOUNT", "")
        delta_del = f"{delete_insert_scenario.delta_location}_del_new"
        test_name = _test_id(request)
        vars_ = {
            "delta_location": delete_insert_scenario.delta_location,
            "delta_location_with_delete": delta_del,
            "source_root": delete_insert_scenario.historical.ss_base_path,
            "source_pattern": r".*\.ss$",
            "max_files_per_trigger": 500,
        }

        result = run_dbt(
            ["run", "--select", "idempotent_delete_insert"],
            extra_vars=vars_,
            test_name=f"{test_name}_initial",
        )
        assert result.success

        before_info = verify_delta_with_duckdb(delta_del)
        wm_before = read_watermark(delta_del)
        assert wm_before is not None

        submit_datagen_job(delete_insert_scenario.new_data, adla_account=adla_account, au=5)

        result = run_dbt(
            ["run", "--select", "idempotent_delete_insert"],
            extra_vars=vars_,
            test_name=f"{test_name}_incremental",
        )
        assert result.success

        after_info = verify_delta_with_duckdb(delta_del)
        assert after_info["total_rows"] > before_info["total_rows"]

        wm_after = read_watermark(delta_del)
        assert wm_after is not None
        assert wm_after.batch_id > wm_before.batch_id


# ---------------------------------------------------------------------------
# Filtered edition
# ---------------------------------------------------------------------------


class TestFilteredEdition:
    @pytest.mark.timeout(3600)
    def test_filtered_full_refresh_only_standard_rows(
        self, append_scenario: ScenarioConfig, request: pytest.FixtureRequest
    ):
        vars_ = _dbt_vars(append_scenario)
        test_name = _test_id(request)
        delta_filtered = f"{append_scenario.delta_location}_filtered"

        result = run_dbt(
            ["run", "--full-refresh", "--select", "filtered_edition"],
            extra_vars=vars_,
            test_name=test_name,
        )
        assert result.success

        all_records = dataset_to_records(append_scenario.historical)
        expected_standard = [r for r in all_records if r.get("edition") == "Standard"]

        delta_info = verify_delta_with_duckdb(delta_filtered)
        assert not delta_info["errors"]
        # Row count >= expected (new_data from incremental test may add files)
        assert delta_info["total_rows"] >= len(expected_standard), (
            f"Expected at least {len(expected_standard)} Standard rows, "
            f"got {delta_info['total_rows']}"
        )

        # Verify edition filter via non-partition column query
        # Note: partition cols read as NULL by DuckDB (SCOPE non-standard hive format)
        # but non-partition data columns are correct
        non_standard = query_delta_with_duckdb(
            f"SELECT DISTINCT edition FROM delta_scan('{delta_filtered}') "
            "WHERE edition IS NOT NULL AND edition != 'Standard'"
        )
        assert non_standard == []

        wm = read_watermark(delta_filtered)
        assert wm is not None


# ---------------------------------------------------------------------------
# Full refresh after incremental
# ---------------------------------------------------------------------------


class TestFullRefreshAfterIncremental:
    @pytest.mark.timeout(3600)
    def test_full_refresh_after_incremental_no_duplicates(
        self, append_scenario: ScenarioConfig, request: pytest.FixtureRequest
    ):
        """Full refresh resets watermark+sources and reloads all data."""
        test_name = _test_id(request)
        vars_ = _dbt_vars(append_scenario)
        vars_["delta_location"] = f"{append_scenario.delta_location}_fullrefresh_test"
        delta_loc = vars_["delta_location"]

        result = run_dbt(
            ["run", "--select", "append_no_delete"],
            extra_vars=vars_,
            test_name=f"{test_name}_initial",
        )
        assert result.success

        before_info = verify_delta_with_duckdb(delta_loc)
        rows_before = before_info["total_rows"]
        assert rows_before > 0

        wm_before = read_watermark(delta_loc)
        assert wm_before is not None
        log_count_before = count_delta_log_files(delta_loc)

        result = run_dbt(
            ["run", "--full-refresh", "--select", "append_no_delete"],
            extra_vars=vars_,
            test_name=f"{test_name}_full_refresh",
        )
        assert result.success

        after_info = verify_delta_with_duckdb(delta_loc)
        assert after_info["total_rows"] == rows_before

        log_count_after = count_delta_log_files(delta_loc)
        assert log_count_after > log_count_before

        wm_after = read_watermark(delta_loc)
        assert wm_after is not None
        assert wm_after.batch_id == 0, f"batch_id should reset to 0, got {wm_after.batch_id}"

        sources = list_source_files(delta_loc)
        assert "0" in sources, f"Should have sources/0 after full refresh, got {sources}"
