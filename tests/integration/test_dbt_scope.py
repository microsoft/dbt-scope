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
    _PREFIX,
    ScenarioConfig,
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
        "delta_location_filtered": f"{scenario.delta_location}_filtered",
        "source_roots": [scenario.historical.ss_base_path],
        "source_patterns": [r".*\.ss$"],
        "max_files_per_trigger": 500,
        "job_tag": f"{scenario.name}_{_PREFIX}",
    }


def _test_id(request: pytest.FixtureRequest) -> str:
    return request.node.name.replace("[", "_").replace("]", "").replace("/", "_")


# ---------------------------------------------------------------------------
# Full refresh (1 SCOPE job)
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
# Incremental: idempotent no-op + new data pickup (2 SCOPE jobs + 1 datagen)
# ---------------------------------------------------------------------------


class TestIncremental:
    @pytest.mark.timeout(3600)
    def test_incremental_idempotent_then_picks_up_new_data(
        self, append_scenario: ScenarioConfig, request: pytest.FixtureRequest
    ):
        """Consolidated incremental test:
        0. Initial run -- processes all files, establishes watermark
        1. Re-run with no new files -- idempotent no-op
        2. Generate new SS data
        3. Re-run -- picks up new files, watermark advances
        """
        vars_ = _dbt_vars(append_scenario)
        # Separate delta_location so parallel xdist workers don't collide
        vars_["delta_location"] = f"{append_scenario.delta_location}_incr"
        delta_loc = vars_["delta_location"]
        adla_account = os.environ.get("SCOPE_ADLA_ACCOUNT", "")
        test_name = _test_id(request)

        # Step 0: Initial run
        result = run_dbt(
            ["run", "--select", "append_no_delete"],
            extra_vars=vars_,
            test_name=f"{test_name}_initial",
        )
        assert result.success

        before_info = verify_delta_with_duckdb(delta_loc)
        assert before_info["total_rows"] > 0

        wm_before = read_watermark(delta_loc)
        assert wm_before is not None

        # Step 1: Re-run with no new files -- idempotent no-op
        result = run_dbt(
            ["run", "--select", "append_no_delete"],
            extra_vars=vars_,
            test_name=f"{test_name}_noop",
        )
        assert result.success

        noop_info = verify_delta_with_duckdb(delta_loc)
        assert noop_info["total_rows"] == before_info["total_rows"], "No-op should not change rows"

        wm_noop = read_watermark(delta_loc)
        assert wm_noop is not None
        assert wm_noop.batch_id == wm_before.batch_id, "batch_id should not change on no-op"
        assert wm_noop.version == wm_before.version, "version should not change on no-op"

        # Step 2: Generate new SS data
        submit_datagen_job(append_scenario.new_data, adla_account=adla_account, au=5)

        # Step 3: Incremental run -- picks up new files
        result = run_dbt(
            ["run", "--select", "append_no_delete"],
            extra_vars=vars_,
            test_name=f"{test_name}_incremental",
        )
        assert result.success

        after_info = verify_delta_with_duckdb(delta_loc)
        assert after_info["total_rows"] > before_info["total_rows"]

        wm_after = read_watermark(delta_loc)
        assert wm_after is not None
        assert wm_after.batch_id > wm_before.batch_id, (
            f"batch_id should advance: {wm_before.batch_id} -> {wm_after.batch_id}"
        )
        assert wm_after.version > wm_before.version

        # Sources for new batch exist
        sources = list_source_files(delta_loc)
        has_new_batch = (
            str(wm_after.batch_id) in sources or f"{wm_after.batch_id}.parquet" in sources
        )
        assert has_new_batch, f"Sources should have batch {wm_after.batch_id}, got {sources}"

        log.info(
            "Incremental passed: rows %d->%d->%d, batch_id %d->%d, sources: %s",
            before_info["total_rows"],
            noop_info["total_rows"],
            after_info["total_rows"],
            wm_before.batch_id,
            wm_after.batch_id,
            sources,
        )


# ---------------------------------------------------------------------------
# Filtered edition (1 SCOPE job)
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
        assert delta_info["total_rows"] >= len(expected_standard), (
            f"Expected at least {len(expected_standard)} Standard rows, "
            f"got {delta_info['total_rows']}"
        )

        non_standard = query_delta_with_duckdb(
            f"SELECT DISTINCT edition FROM delta_scan('{delta_filtered}') "
            "WHERE edition IS NOT NULL AND edition != 'Standard'"
        )
        assert non_standard == []

        wm = read_watermark(delta_filtered)
        assert wm is not None
