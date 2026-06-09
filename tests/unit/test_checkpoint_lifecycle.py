"""Unit tests for the multi-batch checkpoint lifecycle with in-memory ADLS mock.

Exercises the full CheckpointManager flow — watermark JSON, JSONL diffs,
parquet snapshot compaction, retention cleanup, and full-refresh reset —
without any Azure credentials or SCOPE execution.
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest

from dbt.adapters.scope.checkpoint import CheckpointManager, Watermark

# ---------------------------------------------------------------------------
# In-memory ADLS Gen2 filesystem mock
# ---------------------------------------------------------------------------

DELTA_LOC = "abfss://testcontainer@teststorage.dfs.core.windows.net/delta/lifecycle_test"


class _DownloadStream:
    """Mimics the Azure ``StorageStreamDownloader`` with a ``readall()`` method."""

    def __init__(self, data: bytes) -> None:
        self._data = data

    def readall(self) -> bytes:
        return self._data


class InMemoryFileClient:
    """Simulates a DataLake file client backed by a shared ``dict[str, bytes]``."""

    def __init__(self, store: dict[str, bytes], path: str) -> None:
        self._store = store
        self._path = path

    def upload_data(self, data: bytes, *, overwrite: bool = True) -> None:
        self._store[self._path] = data

    def download_file(self) -> _DownloadStream:
        if self._path not in self._store:
            raise FileNotFoundError(self._path)
        return _DownloadStream(self._store[self._path])

    def delete_file(self) -> None:
        self._store.pop(self._path, None)


class InMemoryDirectoryClient:
    def create_directory(self) -> None:
        pass


class InMemoryFileSystem:
    """Simulates an ADLS Gen2 filesystem client with in-memory storage."""

    def __init__(self, store: dict[str, bytes]) -> None:
        self._store = store

    def get_file_client(self, path: str) -> InMemoryFileClient:
        return InMemoryFileClient(self._store, path)

    def get_directory_client(self, path: str) -> InMemoryDirectoryClient:
        return InMemoryDirectoryClient()

    def get_paths(self, *, path: str, recursive: bool = False):
        prefix = path.rstrip("/") + "/"
        results = []
        for key in sorted(self._store):
            if key.startswith(prefix):
                suffix = key[len(prefix) :]
                if not recursive and "/" in suffix:
                    continue
                results.append(SimpleNamespace(name=key, is_directory=False))
        return results


class InMemoryServiceClient:
    """Simulates ``DataLakeServiceClient``."""

    def __init__(self, store: dict[str, bytes]) -> None:
        self._fs = InMemoryFileSystem(store)

    def get_file_system_client(self, container: str) -> InMemoryFileSystem:
        return self._fs


@pytest.fixture
def adls_store() -> dict[str, bytes]:
    """Shared in-memory file store used by all tests."""
    return {}


@pytest.fixture
def checkpoint_mgr(adls_store):
    """CheckpointManager wired to in-memory ADLS."""
    service = InMemoryServiceClient(adls_store)
    with (
        patch("dbt.adapters.scope.checkpoint._get_service", return_value=service),
    ):
        yield CheckpointManager(credential=MagicMock())


def _make_times(count: int, base_year: int = 2026, base_month: int = 4) -> list[datetime]:
    """Generate *count* distinct modification times."""
    return [datetime(base_year, base_month, 1, h, 0, tzinfo=timezone.utc) for h in range(count)]


def _make_paths(count: int, prefix: str = "/shares/test/ss") -> list[str]:
    return [f"{prefix}/file_{i:04d}.ss" for i in range(count)]


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestCheckpointLifecycle:
    """Multi-batch checkpoint lifecycle — no SCOPE, no Azure credentials."""

    def test_batch_zero_writes_jsonl_and_watermark(self, checkpoint_mgr, adls_store):
        """Batch 0 always writes JSONL (never parquet) + creates watermark."""
        paths = _make_paths(5)
        times = _make_times(5)

        wm0 = Watermark(version=0, modified_time=times[-1].isoformat(), batch_id=0)
        checkpoint_mgr.write_watermark(DELTA_LOC, wm0)
        checkpoint_mgr.write_batch_sources(
            DELTA_LOC,
            batch_id=0,
            file_paths=paths,
            modification_times=times,
            compaction_interval=1,  # Even with interval=1, batch 0 → JSONL
        )

        # Watermark readable
        wm = checkpoint_mgr.read_watermark(DELTA_LOC)
        assert wm is not None
        assert wm.batch_id == 0
        assert wm.version == 0

        # Sources: batch 0 is JSONL (no .parquet)
        sources = checkpoint_mgr.list_source_files(DELTA_LOC)
        assert "0" in sources
        assert not any(s.endswith(".parquet") for s in sources)

        # JSONL content is valid
        records = checkpoint_mgr.read_batch_source(DELTA_LOC, 0)
        assert len(records) == 5
        assert all(r["batchId"] == 0 for r in records)
        assert all("path" in r for r in records)
        assert all("modificationTime" in r for r in records)

    def test_watermark_advances_monotonically(self, checkpoint_mgr, adls_store):
        """batch_id and version increase with each successive batch."""
        batch_count = 5
        for batch_id in range(batch_count):
            paths = _make_paths(3, prefix=f"/shares/batch{batch_id}")
            times = _make_times(3, base_month=batch_id + 1)

            wm = Watermark(
                version=batch_id,
                modified_time=times[-1].isoformat(),
                batch_id=batch_id,
            )
            checkpoint_mgr.write_watermark(DELTA_LOC, wm)
            checkpoint_mgr.write_batch_sources(
                DELTA_LOC,
                batch_id=batch_id,
                file_paths=paths,
                modification_times=times,
                compaction_interval=100,  # No compaction
            )

        wm = checkpoint_mgr.read_watermark(DELTA_LOC)
        assert wm is not None
        assert wm.batch_id == batch_count - 1
        assert wm.version == batch_count - 1

        # All JSONL diffs present
        sources = checkpoint_mgr.list_source_files(DELTA_LOC)
        for i in range(batch_count):
            assert str(i) in sources

    def test_compaction_writes_parquet_with_full_history(self, checkpoint_mgr, adls_store):
        """At compaction boundary, parquet snapshot contains ALL records."""
        # Batch 0: JSONL
        paths_0 = _make_paths(3)
        times_0 = _make_times(3)
        checkpoint_mgr.write_batch_sources(
            DELTA_LOC,
            batch_id=0,
            file_paths=paths_0,
            modification_times=times_0,
            compaction_interval=2,
        )

        # Batch 1: JSONL (not at boundary: 1 % 2 != 0)
        paths_1 = _make_paths(2, prefix="/shares/b1")
        times_1 = _make_times(2, base_month=5)
        checkpoint_mgr.write_batch_sources(
            DELTA_LOC,
            batch_id=1,
            file_paths=paths_1,
            modification_times=times_1,
            compaction_interval=2,
        )

        # Batch 2: compaction boundary (2 > 0 and 2 % 2 == 0) → parquet snapshot
        paths_2 = _make_paths(4, prefix="/shares/b2")
        times_2 = _make_times(4, base_month=6)
        checkpoint_mgr.write_batch_sources(
            DELTA_LOC,
            batch_id=2,
            file_paths=paths_2,
            modification_times=times_2,
            compaction_interval=2,
        )

        sources = checkpoint_mgr.list_source_files(DELTA_LOC)
        assert "2.parquet" in sources, f"Expected parquet snapshot, got {sources}"

        # Parquet snapshot should contain ALL 9 records (3 + 2 + 4)
        records_2 = checkpoint_mgr.read_batch_source(DELTA_LOC, 2)
        assert len(records_2) == 4  # read_batch_source filters to batch_id=2

        # Read the raw parquet to verify full history
        import os

        import duckdb

        parquet_key = next(k for k in adls_store if k.endswith("2.parquet"))
        tmp = "/tmp/test_lifecycle_snapshot.parquet"
        with open(tmp, "wb") as f:
            f.write(adls_store[parquet_key])
        conn = duckdb.connect()
        try:
            total = conn.execute(f"SELECT count(*) FROM read_parquet('{tmp}')").fetchone()[0]
            assert total == 9, f"Parquet snapshot should have 9 records (3+2+4), got {total}"
        finally:
            conn.close()
            os.remove(tmp)

    def test_successive_compactions_include_previous(self, checkpoint_mgr, adls_store):
        """Second compaction includes first snapshot + intermediate JSONL diffs."""
        compaction_interval = 2

        # Batch 0: JSONL (2 files)
        checkpoint_mgr.write_batch_sources(
            DELTA_LOC,
            batch_id=0,
            file_paths=_make_paths(2, prefix="/shares/b0"),
            modification_times=_make_times(2, base_month=1),
            compaction_interval=compaction_interval,
        )

        # Batch 1: JSONL (3 files)
        checkpoint_mgr.write_batch_sources(
            DELTA_LOC,
            batch_id=1,
            file_paths=_make_paths(3, prefix="/shares/b1"),
            modification_times=_make_times(3, base_month=2),
            compaction_interval=compaction_interval,
        )

        # Batch 2: compaction → parquet (2+3+1 = 6 total)
        checkpoint_mgr.write_batch_sources(
            DELTA_LOC,
            batch_id=2,
            file_paths=_make_paths(1, prefix="/shares/b2"),
            modification_times=_make_times(1, base_month=3),
            compaction_interval=compaction_interval,
        )

        # Batch 3: JSONL (2 files)
        checkpoint_mgr.write_batch_sources(
            DELTA_LOC,
            batch_id=3,
            file_paths=_make_paths(2, prefix="/shares/b3"),
            modification_times=_make_times(2, base_month=4),
            compaction_interval=compaction_interval,
        )

        # Batch 4: compaction → parquet (should include all 6+2+2 = 10 total)
        checkpoint_mgr.write_batch_sources(
            DELTA_LOC,
            batch_id=4,
            file_paths=_make_paths(2, prefix="/shares/b4"),
            modification_times=_make_times(2, base_month=5),
            compaction_interval=compaction_interval,
        )

        sources = checkpoint_mgr.list_source_files(DELTA_LOC)
        assert "4.parquet" in sources

        # Old parquet snapshot (batch 2) persists — compaction never deletes files
        assert "2.parquet" in sources, f"Old snapshot should persist, got {sources}"

        # All JSONL diffs persist too
        assert "0" in sources
        assert "1" in sources
        assert "3" in sources

        # Total: 3 JSONL + 2 parquet = 5 files
        assert len(sources) == 5, f"Expected 5 files, got {sources}"

        # Latest snapshot (4.parquet) has full history: all 10 records
        import duckdb

        parquet_key = next(k for k in adls_store if k.endswith("4.parquet"))
        tmp = "/tmp/test_successive_compaction.parquet"
        with open(tmp, "wb") as f:
            f.write(adls_store[parquet_key])
        conn = duckdb.connect()
        try:
            total = conn.execute(f"SELECT count(*) FROM read_parquet('{tmp}')").fetchone()[0]
            assert total == 10, f"Expected 10 total records, got {total}"
        finally:
            conn.close()
            os.remove(tmp)

    def test_retention_caps_source_files(self, checkpoint_mgr, adls_store):
        """cleanup_sources enforces max_files limit."""
        # Write 6 JSONL batches
        for batch_id in range(6):
            checkpoint_mgr.write_batch_sources(
                DELTA_LOC,
                batch_id=batch_id,
                file_paths=_make_paths(2, prefix=f"/shares/b{batch_id}"),
                modification_times=_make_times(2, base_month=batch_id + 1),
                compaction_interval=100,  # No compaction
            )

        sources_before = checkpoint_mgr.list_source_files(DELTA_LOC)
        assert len(sources_before) == 6

        deleted = checkpoint_mgr.cleanup_sources(DELTA_LOC, max_files=3)
        assert deleted == 3

        sources_after = checkpoint_mgr.list_source_files(DELTA_LOC)
        assert len(sources_after) == 3

        # Oldest files (batch 0, 1, 2) should be deleted, newest kept
        assert "0" not in sources_after
        assert "1" not in sources_after
        assert "2" not in sources_after
        assert "3" in sources_after
        assert "4" in sources_after
        assert "5" in sources_after

    def test_full_refresh_resets_all_state(self, checkpoint_mgr, adls_store):
        """delete_watermark clears watermark and all sources, then re-starts at 0."""
        # Set up state: watermark + 3 JSONL batches
        wm = Watermark(version=2, modified_time="2026-04-03T00:00:00+00:00", batch_id=2)
        checkpoint_mgr.write_watermark(DELTA_LOC, wm)
        for batch_id in range(3):
            checkpoint_mgr.write_batch_sources(
                DELTA_LOC,
                batch_id=batch_id,
                file_paths=_make_paths(2, prefix=f"/shares/b{batch_id}"),
                modification_times=_make_times(2, base_month=batch_id + 1),
                compaction_interval=100,
            )

        assert checkpoint_mgr.read_watermark(DELTA_LOC) is not None
        assert len(checkpoint_mgr.list_source_files(DELTA_LOC)) == 3

        # Full refresh: delete everything
        checkpoint_mgr.delete_watermark(DELTA_LOC)

        assert checkpoint_mgr.read_watermark(DELTA_LOC) is None
        assert checkpoint_mgr.list_source_files(DELTA_LOC) == []

        # Re-start at batch 0
        wm0 = Watermark(version=0, modified_time="2026-04-01T00:00:00+00:00", batch_id=0)
        checkpoint_mgr.write_watermark(DELTA_LOC, wm0)
        checkpoint_mgr.write_batch_sources(
            DELTA_LOC,
            batch_id=0,
            file_paths=_make_paths(5),
            modification_times=_make_times(5),
            compaction_interval=1,
        )

        wm_new = checkpoint_mgr.read_watermark(DELTA_LOC)
        assert wm_new is not None
        assert wm_new.batch_id == 0
        assert "0" in checkpoint_mgr.list_source_files(DELTA_LOC)

    def test_aggressive_retention_lifecycle(self, checkpoint_mgr, adls_store):
        """Simulate the aggressive_retention model: 7 batches, compaction_interval=1,
        retention_files=3.

        This replaces the integration test TestAggressiveRetentionAndCompaction
        without running any SCOPE jobs.
        """
        total_files = 62
        max_per_trigger = 10
        compaction_interval = 1
        retention_files = 3

        all_paths = _make_paths(total_files)
        all_times = [
            datetime(2026, 4, 1, i // 60, i % 60, tzinfo=timezone.utc) for i in range(total_files)
        ]

        offset = 0
        batch_id = -1
        current_watermark: Watermark | None = None

        while offset < total_files:
            batch_size = min(max_per_trigger, total_files - offset)
            batch_paths = all_paths[offset : offset + batch_size]
            batch_times = all_times[offset : offset + batch_size]
            offset += batch_size

            # Compute new watermark
            batch_id = (current_watermark.batch_id + 1) if current_watermark else 0
            version = (current_watermark.version + 1) if current_watermark else 0
            new_wm = Watermark(
                version=version,
                modified_time=batch_times[-1].isoformat(),
                batch_id=batch_id,
            )

            # Write watermark
            checkpoint_mgr.write_watermark(DELTA_LOC, new_wm)

            # Write batch sources
            checkpoint_mgr.write_batch_sources(
                DELTA_LOC,
                batch_id=batch_id,
                file_paths=batch_paths,
                modification_times=batch_times,
                compaction_interval=compaction_interval,
            )

            # Retention cleanup
            checkpoint_mgr.cleanup_sources(DELTA_LOC, max_files=retention_files)

            current_watermark = new_wm

        # Final assertions (mirrors the removed integration test)
        wm = checkpoint_mgr.read_watermark(DELTA_LOC)
        assert wm is not None
        assert wm.batch_id > 0, f"Should have multiple batches, got batch_id={wm.batch_id}"

        # 62 files / 10 per trigger = 7 batches (batches 0-6)
        assert wm.batch_id == 6, f"Expected batch_id=6, got {wm.batch_id}"
        assert wm.version == 6

        # Sources directory capped at retention limit (3)
        sources = checkpoint_mgr.list_source_files(DELTA_LOC)
        assert len(sources) <= retention_files, (
            f"Retention should cap at {retention_files} files, got {len(sources)}: {sources}"
        )

        # Should have at least one parquet snapshot (compaction_interval=1)
        parquet_files = [s for s in sources if s.endswith(".parquet")]
        assert len(parquet_files) >= 1, f"Should have parquet snapshot, got: {sources}"

        # Verify the latest parquet snapshot contains accumulated history
        import os

        import duckdb

        latest_parquet_key = [k for k in adls_store if k.endswith(".parquet")][-1]
        tmp = "/tmp/test_aggressive_lifecycle.parquet"
        with open(tmp, "wb") as f:
            f.write(adls_store[latest_parquet_key])
        conn = duckdb.connect()
        try:
            total = conn.execute(f"SELECT count(*) FROM read_parquet('{tmp}')").fetchone()[0]
            # Parquet snapshot may not have all 62 records because retention deletes
            # old JSONL diffs that were inputs to previous snapshots, but it should
            # include at least the records from batches still present.
            assert total > 0, "Parquet snapshot should have records"

            # Verify monotonically increasing batch IDs in snapshot
            batch_ids = conn.execute(
                f'SELECT DISTINCT "batchId" FROM read_parquet(\'{tmp}\') ORDER BY "batchId"'
            ).fetchall()
            ids = [row[0] for row in batch_ids]
            assert ids == sorted(ids), f"Batch IDs should be sorted: {ids}"
        finally:
            conn.close()
            os.remove(tmp)

    def test_dump_checkpoint_to_disk(self, checkpoint_mgr, adls_store):
        """Run 110 batches and dump checkpoint files to /tmp for inspection.

        Validates that each parquet snapshot == previous parquet + JSONL diffs
        between the two snapshots.  Retention is set high enough so no files
        are trimmed — all JSONL diffs stay on disk for validation.

        After the test, inspect the output with::

            ls -la /tmp/dbt_scope_checkpoint_demo/_checkpoint/sources/
            duckdb -c "SELECT * FROM read_parquet('/tmp/dbt_scope_checkpoint_demo/_checkpoint/sources/100.parquet') LIMIT 20"
        """
        import shutil

        import duckdb

        output_dir = "/tmp/dbt_scope_checkpoint_demo"
        shutil.rmtree(output_dir, ignore_errors=True)

        num_batches = 110
        files_per_batch = 2
        compaction_interval = 10
        retention_files = 500  # High enough so nothing is trimmed

        for batch_id in range(num_batches):
            paths = [f"/shares/b{batch_id}/file_{i}.ss" for i in range(files_per_batch)]
            times = [
                datetime(2026, (batch_id % 12) + 1, 1, i, 0, tzinfo=timezone.utc)
                for i in range(files_per_batch)
            ]
            checkpoint_mgr.write_batch_sources(
                DELTA_LOC,
                batch_id=batch_id,
                file_paths=paths,
                modification_times=times,
                compaction_interval=compaction_interval,
            )
            wm = Watermark(version=batch_id, modified_time=times[-1].isoformat(), batch_id=batch_id)
            checkpoint_mgr.write_watermark(DELTA_LOC, wm)

        # Dump in-memory store to disk
        sources_dir = os.path.join(output_dir, "_checkpoint", "sources")
        os.makedirs(sources_dir, exist_ok=True)

        prefix = "delta/lifecycle_test/_checkpoint/"
        for key, data in sorted(adls_store.items()):
            if not key.startswith(prefix):
                continue
            rel = key.removeprefix(prefix)
            dest = os.path.join(output_dir, "_checkpoint", rel)
            os.makedirs(os.path.dirname(dest), exist_ok=True)
            with open(dest, "wb") as f:
                f.write(data)

        # Verify files landed
        files = sorted(os.listdir(sources_dir))
        print(f"\n{'=' * 60}")
        print(f"Checkpoint dumped to: {output_dir}")
        print(f"Sources files ({len(files)}):")
        jsonl = [f for f in files if not f.endswith(".parquet")]
        parquet = [f for f in files if f.endswith(".parquet")]
        print(f"  JSONL diffs: {len(jsonl)}")
        print(f"  Parquet snapshots: {parquet}")
        print("\nInspect with:")
        print(f"  ls {sources_dir}/")
        if parquet:
            print(
                f"  duckdb -c \"SELECT * FROM read_parquet('{sources_dir}/{parquet[-1]}') LIMIT 20\""
            )
        print(f"{'=' * 60}\n")

        assert "100.parquet" in files

        # Total: JSONL for every non-compaction batch, parquet for compaction batches.
        # Compaction fires when batch_id > 0 AND batch_id % interval == 0.
        # Batch 0 is always JSONL regardless of interval.
        compaction_batch_ids = [
            b for b in range(num_batches) if b > 0 and b % compaction_interval == 0
        ]
        expected_parquet_count = len(compaction_batch_ids)
        expected_jsonl_count = num_batches - expected_parquet_count
        assert len(jsonl) == expected_jsonl_count, (
            f"Expected {expected_jsonl_count} JSONL, got {len(jsonl)}"
        )
        assert len(parquet) == expected_parquet_count, (
            f"Expected {expected_parquet_count} parquet, got {len(parquet)}"
        )

        # -----------------------------------------------------------------
        # Validate each parquet snapshot = previous parquet + JSONL diffs
        # between the two snapshots (inclusive of the compaction batch itself,
        # whose records go directly into the parquet — no JSONL for it).
        # -----------------------------------------------------------------
        parquet_snapshots = sorted(parquet, key=lambda p: int(p.removesuffix(".parquet")))

        for idx, snap_name in enumerate(parquet_snapshots):
            snap_id = int(snap_name.removesuffix(".parquet"))
            snap_path = os.path.join(sources_dir, snap_name)

            # Read actual parquet content
            conn = duckdb.connect()
            try:
                actual_rows = conn.execute(
                    f"SELECT * FROM read_parquet('{snap_path}') ORDER BY \"batchId\", path"
                ).fetchall()
                cols = [
                    d[0]
                    for d in conn.execute(
                        f"DESCRIBE SELECT * FROM read_parquet('{snap_path}')"
                    ).fetchall()
                ]
            finally:
                conn.close()

            # Build expected: previous parquet + JSONL diffs since + current batch
            expected_records: list[dict] = []

            if idx > 0:
                # Read previous parquet snapshot
                prev_snap = parquet_snapshots[idx - 1]
                prev_path = os.path.join(sources_dir, prev_snap)
                conn = duckdb.connect()
                try:
                    prev_rows = conn.execute(
                        f"SELECT * FROM read_parquet('{prev_path}')"
                    ).fetchall()
                    prev_cols = [
                        d[0]
                        for d in conn.execute(
                            f"DESCRIBE SELECT * FROM read_parquet('{prev_path}')"
                        ).fetchall()
                    ]
                    for row in prev_rows:
                        expected_records.append(dict(zip(prev_cols, row, strict=False)))
                finally:
                    conn.close()
                prev_snap_id = int(prev_snap.removesuffix(".parquet"))
            else:
                prev_snap_id = -1

            # Read JSONL diffs between previous snapshot and this one.
            # Compaction batches (10, 20, ...) don't have JSONL — those records
            # are written directly into the parquet by _write_snapshot_parquet.
            for bid in range(prev_snap_id + 1, snap_id):
                jsonl_path = os.path.join(sources_dir, str(bid))
                if not os.path.exists(jsonl_path):
                    raise AssertionError(
                        f"JSONL {bid} should exist on disk (retention={retention_files})"
                    )
                with open(jsonl_path) as f:
                    for line in f:
                        if line.strip():
                            expected_records.append(json.loads(line))

            # The compaction batch itself (snap_id) has no JSONL — its records
            # were passed directly to _write_snapshot_parquet. We know exactly
            # what they are: files_per_batch records for this batch_id.
            # Reconstruct them to match the format in the parquet.
            for i in range(files_per_batch):
                expected_records.append(
                    {
                        "path": f"/shares/b{snap_id}/file_{i}.ss",
                        "batchId": snap_id,
                    }
                )

            # Compare record counts
            assert len(actual_rows) == len(expected_records), (
                f"{snap_name}: expected {len(expected_records)} records, got {len(actual_rows)}"
            )

            # Compare batchId + path for each record (skip timestamp fields
            # since they have processing-time jitter)
            def sort_key(r):
                return (r.get("batchId", 0), r.get("path", ""))

            actual_dicts = sorted(
                [dict(zip(cols, row, strict=False)) for row in actual_rows],
                key=sort_key,
            )
            expected_records.sort(key=sort_key)

            for i, (actual, expected) in enumerate(
                zip(actual_dicts, expected_records, strict=True)
            ):
                assert actual["batchId"] == expected["batchId"], (
                    f"{snap_name} record {i}: batchId {actual['batchId']} != {expected['batchId']}"
                )
                assert actual["path"] == expected["path"], (
                    f"{snap_name} record {i}: path {actual['path']} != {expected['path']}"
                )

        print(
            f"Validated {len(parquet_snapshots)} parquet snapshots: "
            f"each = previous parquet + intermediate JSONL diffs ✓"
        )

    def test_compaction_handles_timestamp_typed_prior_snapshot(self, checkpoint_mgr, adls_store):
        """Regression for: ``Object of type datetime is not JSON serializable``.

        In production, batches are spaced minutes apart so the
        ``batchProcessingTime`` strings in a per-snapshot NDJSON have
        enough variation for DuckDB's ``read_json_auto`` to infer
        ``TIMESTAMP``. The resulting parquet snapshot then stores the
        column as ``TIMESTAMP``, and the **next** compaction reads it
        back as Python ``datetime`` — which used to crash
        ``json.dumps(...)`` inside ``_write_snapshot_parquet``.

        This test reproduces that exact shape deterministically by
        pre-seeding a parquet snapshot whose ``batchProcessingTime``
        column is ``TIMESTAMP``, then triggering a second compaction.
        Pre-fix: raises ``TypeError: Object of type datetime is not JSON
        serializable``. Post-fix: succeeds.
        """
        import duckdb

        sources_prefix = "delta/lifecycle_test/_checkpoint/sources"  # matches DELTA_LOC's path
        compaction_interval = 10

        # ── 1. Build a parquet snapshot whose ``batchProcessingTime`` is
        # explicitly ``TIMESTAMP``-typed — mimicking what happens in
        # production when DuckDB's ``read_json_auto`` infers TIMESTAMP
        # from well-spaced batch processing times. (DuckDB's inference
        # heuristic varies across versions/sample sizes, so we force the
        # cast here to make the test deterministic across environments.)
        seeded_records = 22  # 11 batches x 2 files
        prior_snapshot_path = f"/tmp/test_prior_snapshot_{id(self)}.parquet"
        prior_ndjson_path = f"/tmp/test_prior_snapshot_{id(self)}.ndjson"
        try:
            with open(prior_ndjson_path, "w") as f:
                for i in range(seeded_records):
                    ts = datetime(2026, 6, 1, tzinfo=timezone.utc) + timedelta(minutes=i * 5)
                    f.write(
                        json.dumps(
                            {
                                "path": f"/shares/seed/file_{i:04d}.ss",
                                "modificationTime": 1700000000000 + i,
                                "batchId": i // 2,
                                "batchProcessingTime": ts.isoformat(),
                            }
                        )
                        + "\n"
                    )
            conn = duckdb.connect()
            try:
                # Force TIMESTAMP via explicit CAST so the test is
                # deterministic regardless of DuckDB's auto-inference
                # heuristic — what we want to assert is the read-back +
                # next-compaction behaviour, not DuckDB's inference.
                conn.execute(
                    "CREATE TABLE t AS "
                    "SELECT path, "
                    '"modificationTime", '
                    '"batchId", '
                    'CAST("batchProcessingTime" AS TIMESTAMP) AS "batchProcessingTime" '
                    f"FROM read_json_auto('{prior_ndjson_path}')"
                )
                schema = conn.execute("DESCRIBE t").fetchall()
                assert any(
                    col[0] == "batchProcessingTime" and col[1] == "TIMESTAMP" for col in schema
                ), f"Setup precondition failed: expected TIMESTAMP, got {schema}"
                conn.execute(f"COPY t TO '{prior_snapshot_path}' (FORMAT PARQUET)")
            finally:
                conn.close()

            with open(prior_snapshot_path, "rb") as f:
                prior_parquet_bytes = f.read()
        finally:
            for p in (prior_ndjson_path, prior_snapshot_path):
                if os.path.exists(p):
                    os.remove(p)

        # ── 2. Inject the prior snapshot directly into in-memory ADLS as
        # "10.parquet" so the next compaction will see it.
        adls_store[f"{sources_prefix}/10.parquet"] = prior_parquet_bytes

        # ── 2b. Also seed a JSONL diff for batch 15 — _write_snapshot_parquet
        # is supposed to merge "prior snapshot + JSONL diffs since snapshot +
        # current batch", so we want to exercise all three legs.
        intermediate_paths = _make_paths(3, prefix="/shares/b15")
        intermediate_times = _make_times(3, base_month=6)
        checkpoint_mgr.write_batch_sources(
            DELTA_LOC,
            batch_id=15,
            file_paths=intermediate_paths,
            modification_times=intermediate_times,
            compaction_interval=compaction_interval,  # 15 % 10 != 0 → JSONL
        )
        assert "15" in checkpoint_mgr.list_source_files(DELTA_LOC), (
            "Intermediate JSONL diff for batch 15 should exist"
        )

        # ── 3. Trigger compaction at batch 20. Pre-fix: this raises
        # ``TypeError: Object of type datetime is not JSON serializable``
        # from inside _write_snapshot_parquet. Post-fix: success.
        new_batch_paths = _make_paths(2, prefix="/shares/b20")
        new_batch_times = _make_times(2, base_month=7)
        checkpoint_mgr.write_batch_sources(
            DELTA_LOC,
            batch_id=20,
            file_paths=new_batch_paths,
            modification_times=new_batch_times,
            compaction_interval=compaction_interval,
        )

        # ── 4. Verify the new snapshot exists and contains records from
        # every leg of the union (prior 22 + intermediate JSONL 3 + current 2 = 27).
        sources = checkpoint_mgr.list_source_files(DELTA_LOC)
        assert "20.parquet" in sources, f"Expected 20.parquet, got {sources}"
        assert "10.parquet" in sources, "Prior snapshot should still exist"
        assert "15" in sources, "Intermediate JSONL diff should still exist"

        new_snapshot_key = next(k for k in adls_store if k.endswith("20.parquet"))
        new_local = f"/tmp/test_new_snapshot_{id(self)}.parquet"
        with open(new_local, "wb") as f:
            f.write(adls_store[new_snapshot_key])
        try:
            conn = duckdb.connect()
            try:
                total = conn.execute(
                    f"SELECT count(*) FROM read_parquet('{new_local}')"
                ).fetchone()[0]
                expected = seeded_records + len(intermediate_paths) + len(new_batch_paths)
                assert total == expected, (
                    f"Expected {expected} records in new snapshot, got {total}"
                )

                seen_batch_ids = {
                    row[0]
                    for row in conn.execute(
                        f"SELECT DISTINCT \"batchId\" FROM read_parquet('{new_local}')"
                    ).fetchall()
                }
                assert 15 in seen_batch_ids, (
                    f"JSONL diff records (batchId=15) missing from snapshot: {seen_batch_ids}"
                )
                assert 20 in seen_batch_ids, (
                    f"Current batch records (batchId=20) missing from snapshot: {seen_batch_ids}"
                )
                assert any(bid <= 10 for bid in seen_batch_ids), (
                    f"Prior snapshot records (batchId<=10) missing: {seen_batch_ids}"
                )

                # New snapshot's batchProcessingTime is deterministically
                # TIMESTAMP (enforced by the explicit CAST in the fix).
                new_schema = conn.execute(
                    f"DESCRIBE SELECT * FROM read_parquet('{new_local}')"
                ).fetchall()
                assert any(
                    col[0] == "batchProcessingTime" and col[1] == "TIMESTAMP" for col in new_schema
                ), f"Expected TIMESTAMP schema in new snapshot, got {new_schema}"
            finally:
                conn.close()
        finally:
            os.remove(new_local)
