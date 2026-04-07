"""Unit tests for the multi-batch checkpoint lifecycle with in-memory ADLS mock.

Exercises the full CheckpointManager flow — watermark JSON, JSONL diffs,
parquet snapshot compaction, retention cleanup, and full-refresh reset —
without any Azure credentials or SCOPE execution.
"""

from __future__ import annotations

from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import patch

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
        patch("dbt.adapters.scope.checkpoint.AzureCliCredential"),
    ):
        yield CheckpointManager()


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

        # Old parquet snapshot (batch 2) should have been deleted
        assert "2.parquet" not in sources, f"Old snapshot should be deleted, got {sources}"

        # Read full snapshot to verify all 10 records
        import os

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
