"""Tests for multi-source (cross-product) file discovery logic.

These tests verify the cross-product UNION + deduplication behavior
that is implemented in ScopeAdapter.discover_files(). Since ScopeAdapter
requires heavy infrastructure, we test the core logic patterns here
using FileTracker + mocks.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock

from dbt.adapters.scope.adls_gen1_client import FileInfo
from dbt.adapters.scope.checkpoint import Watermark
from dbt.adapters.scope.file_tracker import FileTracker


def _make_file(path: str, mod_time: datetime, length: int = 1000) -> FileInfo:
    return FileInfo(
        path=path,
        name=path.rsplit("/", 1)[-1],
        length=length,
        modification_time=mod_time,
    )


NOW = datetime(2026, 4, 1, 12, 0, 0, tzinfo=timezone.utc)


def _discover_multi_source(
    gen1_mock: MagicMock,
    checkpoint_mock: MagicMock,
    source_roots: list[str],
    source_patterns: list[str],
    watermark: Watermark | None = None,
    max_files_per_trigger: int = 50,
    safety_buffer_seconds: int = 30,
) -> list[str]:
    """Simulate the cross-product discovery logic from ScopeAdapter.discover_files()."""
    tracker = FileTracker(gen1_mock, checkpoint_mock)

    seen_paths: set[str] = set()
    all_unprocessed: list[FileInfo] = []

    for root in source_roots:
        for pattern in source_patterns:
            unprocessed = tracker.discover_unprocessed_files(
                root=root,
                pattern=pattern,
                watermark=watermark,
                safety_buffer_seconds=safety_buffer_seconds,
            )
            for f in unprocessed:
                if f.path not in seen_paths:
                    seen_paths.add(f.path)
                    all_unprocessed.append(f)

    all_unprocessed.sort(key=lambda f: f.modification_time)
    batch = FileTracker.get_next_batch(all_unprocessed, max_files_per_trigger)
    return [f.path for f in batch]


class TestMultiSourceCrossProduct:
    """Test cross-product of source_roots x source_patterns."""

    def test_single_root_single_pattern(self):
        """Degenerate case: 1x1 behaves like the old single-source logic."""
        files = [
            _make_file("/root1/a.ss", NOW - timedelta(hours=2)),
            _make_file("/root1/b.ss", NOW - timedelta(hours=1)),
        ]
        gen1 = MagicMock()
        gen1.list_files.return_value = files
        checkpoint = MagicMock()

        result = _discover_multi_source(
            gen1,
            checkpoint,
            source_roots=["/root1"],
            source_patterns=[r".*\.ss$"],
        )
        assert len(result) == 2
        assert "/root1/a.ss" in result
        assert "/root1/b.ss" in result

    def test_two_roots_one_pattern(self):
        """Two roots x one pattern = two discover calls, union results."""
        root1_files = [_make_file("/root1/a.ss", NOW - timedelta(hours=2))]
        root2_files = [_make_file("/root2/b.ss", NOW - timedelta(hours=1))]

        gen1 = MagicMock()
        gen1.list_files.side_effect = [root1_files, root2_files]
        checkpoint = MagicMock()

        result = _discover_multi_source(
            gen1,
            checkpoint,
            source_roots=["/root1", "/root2"],
            source_patterns=[r".*\.ss$"],
        )
        assert len(result) == 2
        assert "/root1/a.ss" in result
        assert "/root2/b.ss" in result

    def test_one_root_two_patterns(self):
        """One root x two patterns = two discover calls with different patterns."""
        ss_files = [_make_file("/root1/a.ss", NOW - timedelta(hours=2))]
        csv_files = [_make_file("/root1/b.csv", NOW - timedelta(hours=1))]

        gen1 = MagicMock()
        gen1.list_files.side_effect = [ss_files, csv_files]
        checkpoint = MagicMock()

        result = _discover_multi_source(
            gen1,
            checkpoint,
            source_roots=["/root1"],
            source_patterns=[r".*\.ss$", r".*\.csv$"],
        )
        assert len(result) == 2
        assert "/root1/a.ss" in result
        assert "/root1/b.csv" in result

    def test_two_roots_two_patterns(self):
        """Full cross-product: 2x2 = 4 discover calls."""
        files = [
            [_make_file("/r1/a.ss", NOW - timedelta(hours=4))],
            [_make_file("/r1/b.csv", NOW - timedelta(hours=3))],
            [_make_file("/r2/c.ss", NOW - timedelta(hours=2))],
            [_make_file("/r2/d.csv", NOW - timedelta(hours=1))],
        ]
        gen1 = MagicMock()
        gen1.list_files.side_effect = files
        checkpoint = MagicMock()

        result = _discover_multi_source(
            gen1,
            checkpoint,
            source_roots=["/r1", "/r2"],
            source_patterns=[r".*\.ss$", r".*\.csv$"],
        )
        assert len(result) == 4
        assert gen1.list_files.call_count == 4


class TestMultiSourceDeduplication:
    """Test deduplication when cross-product yields the same file."""

    def test_duplicate_file_from_overlapping_patterns(self):
        """Same file matched by two patterns → appears once."""
        shared_file = _make_file("/root1/data.ss", NOW - timedelta(hours=1))

        gen1 = MagicMock()
        # Both patterns return the same file
        gen1.list_files.side_effect = [[shared_file], [shared_file]]
        checkpoint = MagicMock()

        result = _discover_multi_source(
            gen1,
            checkpoint,
            source_roots=["/root1"],
            source_patterns=[r".*\.ss$", r"data.*"],
        )
        assert len(result) == 1
        assert result[0] == "/root1/data.ss"

    def test_duplicate_across_roots_same_path(self):
        """Edge case: different roots but file resolves to the same path."""
        file1 = _make_file("/shares/data/a.ss", NOW - timedelta(hours=1))

        gen1 = MagicMock()
        gen1.list_files.side_effect = [[file1], [file1]]
        checkpoint = MagicMock()

        result = _discover_multi_source(
            gen1,
            checkpoint,
            source_roots=["/shares/data", "/shares/data"],
            source_patterns=[r".*\.ss$"],
        )
        assert len(result) == 1

    def test_no_duplicates_different_paths(self):
        """Files with different paths from overlapping patterns → all kept."""
        f1 = _make_file("/root1/a.ss", NOW - timedelta(hours=2))
        f2 = _make_file("/root1/b.ss", NOW - timedelta(hours=1))

        gen1 = MagicMock()
        gen1.list_files.side_effect = [[f1], [f2]]
        checkpoint = MagicMock()

        result = _discover_multi_source(
            gen1,
            checkpoint,
            source_roots=["/root1"],
            source_patterns=[r"a\.ss$", r"b\.ss$"],
        )
        assert len(result) == 2


class TestMultiSourceEdgeCases:
    """Edge cases for multi-source discovery."""

    def test_empty_roots_returns_empty(self):
        gen1 = MagicMock()
        checkpoint = MagicMock()

        result = _discover_multi_source(
            gen1,
            checkpoint,
            source_roots=[],
            source_patterns=[r".*\.ss$"],
        )
        assert result == []
        gen1.list_files.assert_not_called()

    def test_empty_patterns_returns_empty(self):
        gen1 = MagicMock()
        checkpoint = MagicMock()

        result = _discover_multi_source(
            gen1,
            checkpoint,
            source_roots=["/root1"],
            source_patterns=[],
        )
        assert result == []
        gen1.list_files.assert_not_called()

    def test_empty_roots_and_patterns_returns_empty(self):
        gen1 = MagicMock()
        checkpoint = MagicMock()

        result = _discover_multi_source(
            gen1,
            checkpoint,
            source_roots=[],
            source_patterns=[],
        )
        assert result == []

    def test_max_files_per_trigger_limits_batch(self):
        """Cross-product yields many files but batch is capped."""
        files = [_make_file(f"/root1/{i}.ss", NOW - timedelta(hours=100 - i)) for i in range(20)]

        gen1 = MagicMock()
        gen1.list_files.return_value = files
        checkpoint = MagicMock()

        result = _discover_multi_source(
            gen1,
            checkpoint,
            source_roots=["/root1"],
            source_patterns=[r".*\.ss$"],
            max_files_per_trigger=5,
        )
        assert len(result) == 5

    def test_results_sorted_by_modification_time(self):
        """Final results are sorted by modification_time (oldest first)."""
        newer = _make_file("/root2/new.ss", NOW - timedelta(hours=1))
        older = _make_file("/root1/old.ss", NOW - timedelta(hours=3))

        gen1 = MagicMock()
        # root2 returns newer file first, root1 returns older
        gen1.list_files.side_effect = [[newer], [older]]
        checkpoint = MagicMock()

        result = _discover_multi_source(
            gen1,
            checkpoint,
            source_roots=["/root2", "/root1"],
            source_patterns=[r".*\.ss$"],
        )
        assert result == ["/root1/old.ss", "/root2/new.ss"]
