"""Unit tests for starting_timestamp support in file discovery."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock

import pytest
from dbt_common.exceptions import DbtRuntimeError

from dbt.adapters.scope.adls_gen1_client import FileInfo
from dbt.adapters.scope.checkpoint import Watermark
from dbt.adapters.scope.file_tracker import FileTracker
from dbt.adapters.scope.impl import _parse_starting_timestamp

NOW = datetime(2026, 4, 7, 12, 0, 0, tzinfo=timezone.utc)


def _make_file(path: str, mod_time: datetime, length: int = 1000) -> FileInfo:
    return FileInfo(
        path=path,
        name=path.rsplit("/", 1)[-1],
        length=length,
        modification_time=mod_time,
    )


def _discover_with_starting_timestamp(
    gen1_mock: MagicMock,
    checkpoint_mock: MagicMock,
    source_roots: list[str],
    source_patterns: list[str],
    starting_timestamp: str | None = None,
    watermark: Watermark | None = None,
    max_files_per_trigger: int = 50,
    safety_buffer_seconds: int = 0,
) -> list[str]:
    """Simulate ScopeAdapter.discover_files() logic with starting_timestamp.

    Reproduces the core logic from impl.py without needing a full adapter.
    """
    starting_ts_dt = _parse_starting_timestamp(starting_timestamp) if starting_timestamp else None
    tracker = FileTracker(gen1_mock, checkpoint_mock)

    used_starting_timestamp = False
    if watermark is not None:
        effective_watermark = watermark
    elif starting_ts_dt is not None:
        effective_watermark = Watermark(modified_time=starting_ts_dt.isoformat())
        used_starting_timestamp = True
    else:
        effective_watermark = None

    seen_paths: set[str] = set()
    all_unprocessed: list[FileInfo] = []

    for root in source_roots:
        for pattern in source_patterns:
            unprocessed = tracker.discover_unprocessed_files(
                root=root,
                pattern=pattern,
                watermark=effective_watermark,
                safety_buffer_seconds=safety_buffer_seconds,
            )
            for f in unprocessed:
                if f.path not in seen_paths:
                    seen_paths.add(f.path)
                    all_unprocessed.append(f)

    if used_starting_timestamp and not all_unprocessed:
        # Check if there are any files at all (re-list without watermark)
        for root in source_roots:
            for pattern in source_patterns:
                files = tracker.discover_unprocessed_files(
                    root=root, pattern=pattern, watermark=None, safety_buffer_seconds=0
                )
                if files:
                    raise DbtRuntimeError(
                        f"starting_timestamp '{starting_timestamp}' is after all available "
                        f"source files."
                    )

    all_unprocessed.sort(key=lambda f: f.modification_time)
    batch = FileTracker.get_next_batch(all_unprocessed, max_files_per_trigger)
    return [f.path for f in batch]


class TestParseStartingTimestamp:
    def test_valid_utc_timestamp(self):
        dt = _parse_starting_timestamp("2026-04-07T10:00:00+00:00")
        assert dt.year == 2026
        assert dt.month == 4
        assert dt.tzinfo is not None

    def test_valid_with_offset(self):
        dt = _parse_starting_timestamp("2026-04-07T10:00:00-05:00")
        assert dt.tzinfo == timezone.utc
        assert dt.hour == 15  # converted to UTC

    def test_invalid_string_raises(self):
        with pytest.raises(DbtRuntimeError, match="Invalid starting_timestamp"):
            _parse_starting_timestamp("not-a-date")

    def test_empty_string_raises(self):
        with pytest.raises(DbtRuntimeError, match="Invalid starting_timestamp"):
            _parse_starting_timestamp("")

    def test_naive_timestamp_raises(self):
        with pytest.raises(DbtRuntimeError, match="missing timezone info"):
            _parse_starting_timestamp("2026-04-07T10:00:00")


class TestStartingTimestampFiltering:
    def test_used_when_no_watermark(self):
        """starting_timestamp creates a synthetic watermark that filters old files."""
        old = _make_file("/root/old.ss", NOW - timedelta(days=30))
        new = _make_file("/root/new.ss", NOW - timedelta(hours=1))

        gen1 = MagicMock()
        gen1.list_files.return_value = [old, new]
        checkpoint = MagicMock()

        ts = (NOW - timedelta(days=1)).isoformat()
        result = _discover_with_starting_timestamp(
            gen1,
            checkpoint,
            source_roots=["/root"],
            source_patterns=[r".*\.ss$"],
            starting_timestamp=ts,
            watermark=None,
        )
        assert len(result) == 1
        assert result[0] == "/root/new.ss"

    def test_ignored_when_watermark_exists(self):
        """starting_timestamp is a no-op when a checkpoint watermark exists."""
        old = _make_file("/root/old.ss", NOW - timedelta(days=30))
        new = _make_file("/root/new.ss", NOW - timedelta(hours=1))

        gen1 = MagicMock()
        gen1.list_files.return_value = [old, new]
        checkpoint = MagicMock()

        # Watermark is before old file — both should be returned
        wm = Watermark(
            version=1,
            modified_time=(NOW - timedelta(days=60)).isoformat(),
            batch_id=5,
        )
        # starting_timestamp would skip old file, but watermark takes precedence
        ts = (NOW - timedelta(days=1)).isoformat()
        result = _discover_with_starting_timestamp(
            gen1,
            checkpoint,
            source_roots=["/root"],
            source_patterns=[r".*\.ss$"],
            starting_timestamp=ts,
            watermark=wm,
        )
        assert len(result) == 2

    def test_valid_timestamp_filters_correctly(self):
        """Files at or before starting_timestamp are excluded; files after are included."""
        t1 = NOW - timedelta(days=10)
        t2 = NOW - timedelta(days=5)
        t3 = NOW - timedelta(days=1)
        files = [
            _make_file("/root/a.ss", t1),
            _make_file("/root/b.ss", t2),
            _make_file("/root/c.ss", t3),
        ]

        gen1 = MagicMock()
        gen1.list_files.return_value = files
        checkpoint = MagicMock()

        # Timestamp between t2 and t3 — only c.ss should pass
        ts = (NOW - timedelta(days=3)).isoformat()
        result = _discover_with_starting_timestamp(
            gen1,
            checkpoint,
            source_roots=["/root"],
            source_patterns=[r".*\.ss$"],
            starting_timestamp=ts,
        )
        assert result == ["/root/c.ss"]


class TestStartingTimestampValidation:
    def test_invalid_timestamp_raises(self):
        """Garbage timestamp string raises DbtRuntimeError."""
        gen1 = MagicMock()
        checkpoint = MagicMock()

        with pytest.raises(DbtRuntimeError, match="Invalid starting_timestamp"):
            _discover_with_starting_timestamp(
                gen1,
                checkpoint,
                source_roots=["/root"],
                source_patterns=[r".*\.ss$"],
                starting_timestamp="garbage",
            )

    def test_timestamp_after_all_files_raises(self):
        """Timestamp after all available files raises DbtRuntimeError."""
        files = [
            _make_file("/root/a.ss", NOW - timedelta(days=10)),
            _make_file("/root/b.ss", NOW - timedelta(days=5)),
        ]

        gen1 = MagicMock()
        gen1.list_files.return_value = files
        checkpoint = MagicMock()

        # Timestamp is after all files
        ts = (NOW + timedelta(days=1)).isoformat()
        with pytest.raises(DbtRuntimeError, match="after all available"):
            _discover_with_starting_timestamp(
                gen1,
                checkpoint,
                source_roots=["/root"],
                source_patterns=[r".*\.ss$"],
                starting_timestamp=ts,
            )

    def test_no_files_at_all_returns_empty(self):
        """Empty source directory is not an error even with starting_timestamp."""
        gen1 = MagicMock()
        gen1.list_files.return_value = []
        checkpoint = MagicMock()

        ts = (NOW - timedelta(days=1)).isoformat()
        result = _discover_with_starting_timestamp(
            gen1,
            checkpoint,
            source_roots=["/root"],
            source_patterns=[r".*\.ss$"],
            starting_timestamp=ts,
        )
        assert result == []

    def test_none_starting_timestamp_processes_all(self):
        """When starting_timestamp is None, all files are processed (backward compat)."""
        files = [
            _make_file("/root/a.ss", NOW - timedelta(days=10)),
            _make_file("/root/b.ss", NOW - timedelta(hours=1)),
        ]

        gen1 = MagicMock()
        gen1.list_files.return_value = files
        checkpoint = MagicMock()

        result = _discover_with_starting_timestamp(
            gen1,
            checkpoint,
            source_roots=["/root"],
            source_patterns=[r".*\.ss$"],
            starting_timestamp=None,
        )
        assert len(result) == 2
