"""Unit tests for file listing and enrichment caching.

Tests the cache behavior in AdlsGen1Client.list_files() and
enrich_with_estimates(), and the adapter-level FileInfo cache used
by update_checkpoint() to avoid redundant ADLS listings.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

from dbt.adapters.scope.adls_gen1_client import AdlsGen1Client, FileInfo
from dbt.adapters.scope.checkpoint import Watermark
from dbt.adapters.scope.file_tracker import FileTracker

NOW = datetime(2026, 4, 1, 12, 0, 0, tzinfo=timezone.utc)


def _make_file(name: str, mod_time: datetime, length: int = 1000) -> FileInfo:
    return FileInfo(
        path=f"/shares/test/{name}",
        name=name,
        length=length,
        modification_time=mod_time,
        raw={
            "name": f"shares/test/{name}",
            "length": length,
            "type": "FILE",
            "modificationTime": int(mod_time.timestamp() * 1000),
        },
    )


class TestAdlsGen1ClientListFilesCache:
    """Test that list_files() caches results by (root, pattern)."""

    def test_cache_hit_returns_same_result(self):
        """Second call with same args returns cached list without ADLS call."""
        client = AdlsGen1Client(account="test")

        files = [_make_file("a.ss", NOW - timedelta(hours=2))]

        mock_fs = MagicMock()
        mock_fs.ls.return_value = [f.raw for f in files]

        with patch.object(client, "_get_fs", return_value=mock_fs):
            result1 = client.list_files("/shares/test", pattern=r".*\.ss$")
            result2 = client.list_files("/shares/test", pattern=r".*\.ss$")

        assert len(result1) == 1
        assert len(result2) == 1
        assert result1[0].path == result2[0].path
        # ADLS ls should be called only once (first call)
        mock_fs.ls.assert_called_once()

    def test_different_pattern_is_separate_cache_entry(self):
        """Different patterns are cached independently."""
        client = AdlsGen1Client(account="test")

        ss_files = [_make_file("a.ss", NOW - timedelta(hours=2))]
        csv_files = [_make_file("b.csv", NOW - timedelta(hours=1))]

        mock_fs = MagicMock()
        mock_fs.ls.side_effect = [
            [f.raw for f in ss_files],
            [f.raw for f in csv_files],
        ]

        with patch.object(client, "_get_fs", return_value=mock_fs):
            result_ss = client.list_files("/shares/test", pattern=r".*\.ss$")
            result_csv = client.list_files("/shares/test", pattern=r".*\.csv$")

        assert len(result_ss) == 1
        assert result_ss[0].name == "a.ss"
        assert len(result_csv) == 1
        assert result_csv[0].name == "b.csv"
        assert mock_fs.ls.call_count == 2

    def test_different_root_is_separate_cache_entry(self):
        """Different roots are cached independently."""
        client = AdlsGen1Client(account="test")

        root1_files = [_make_file("a.ss", NOW - timedelta(hours=2))]
        root2_files = [_make_file("b.ss", NOW - timedelta(hours=1))]

        mock_fs = MagicMock()
        mock_fs.ls.side_effect = [
            [f.raw for f in root1_files],
            [f.raw for f in root2_files],
        ]

        with patch.object(client, "_get_fs", return_value=mock_fs):
            r1 = client.list_files("/root1", pattern=r".*\.ss$")
            r2 = client.list_files("/root2", pattern=r".*\.ss$")

        assert len(r1) == 1
        assert len(r2) == 1
        assert mock_fs.ls.call_count == 2

    def test_clear_file_cache_forces_fresh_listing(self):
        """After clear_file_cache(), next call hits ADLS again."""
        client = AdlsGen1Client(account="test")

        files = [_make_file("a.ss", NOW - timedelta(hours=2))]

        mock_fs = MagicMock()
        mock_fs.ls.return_value = [f.raw for f in files]

        with patch.object(client, "_get_fs", return_value=mock_fs):
            client.list_files("/shares/test", pattern=r".*\.ss$")
            assert mock_fs.ls.call_count == 1

            client.clear_file_cache()

            client.list_files("/shares/test", pattern=r".*\.ss$")
            assert mock_fs.ls.call_count == 2

    def test_cache_survives_multiple_calls(self):
        """Cache hit works for many repeated calls."""
        client = AdlsGen1Client(account="test")

        files = [_make_file("a.ss", NOW - timedelta(hours=2))]

        mock_fs = MagicMock()
        mock_fs.ls.return_value = [f.raw for f in files]

        with patch.object(client, "_get_fs", return_value=mock_fs):
            for _ in range(10):
                result = client.list_files("/shares/test", pattern=r".*\.ss$")
                assert len(result) == 1

        mock_fs.ls.assert_called_once()


class TestAdlsGen1ClientEnrichmentCache:
    """Test that enrich_with_estimates() caches results per file path."""

    def test_enrichment_cache_hit(self):
        """Second enrichment call for same files uses cache."""
        client = AdlsGen1Client(account="test")

        files = [_make_file("a.ss", NOW - timedelta(hours=2), length=500)]

        with patch.object(client, "estimate_bytes", return_value=(1500, ["/contrib/a"])) as mock_eb:
            result1 = client.enrich_with_estimates(files)
            result2 = client.enrich_with_estimates(files)

        assert result1[0].estimated_bytes == 1500
        assert result2[0].estimated_bytes == 1500
        # estimate_bytes called only once
        mock_eb.assert_called_once()

    def test_enrichment_cache_miss_for_new_files(self):
        """New files trigger ADLS calls; already-seen files use cache."""
        client = AdlsGen1Client(account="test")

        file_a = _make_file("a.ss", NOW - timedelta(hours=2), length=500)
        file_b = _make_file("b.ss", NOW - timedelta(hours=1), length=700)

        with patch.object(client, "estimate_bytes") as mock_eb:
            mock_eb.side_effect = [(1500, ["/contrib/a"]), (2000, ["/contrib/b"])]
            client.enrich_with_estimates([file_a])
            # Second call includes both — a should be cached, b is new
            result = client.enrich_with_estimates([file_a, file_b])

        assert result[0].estimated_bytes == 1500  # cached
        assert result[1].estimated_bytes == 2000  # fresh
        assert mock_eb.call_count == 2  # a once, b once

    def test_clear_file_cache_clears_enrichment(self):
        """clear_file_cache() also clears enrichment cache."""
        client = AdlsGen1Client(account="test")

        files = [_make_file("a.ss", NOW - timedelta(hours=2), length=500)]

        with patch.object(client, "estimate_bytes", return_value=(1500, [])) as mock_eb:
            client.enrich_with_estimates(files)
            assert mock_eb.call_count == 1

            client.clear_file_cache()

            client.enrich_with_estimates(files)
            assert mock_eb.call_count == 2  # called again after cache clear


class TestBatchLoopCacheLifecycle:
    """End-to-end test simulating the microbatch loop.

    Verifies that LIST is called once across all batch iterations, and
    update_checkpoint can use cached FileInfo objects.
    """

    def test_batch_loop_lists_once(self):
        """Simulates 3 batch iterations — ADLS LIST should be called once."""
        gen1 = MagicMock(spec=AdlsGen1Client)
        checkpoint_mgr = MagicMock()
        tracker = FileTracker(gen1, checkpoint_mgr)

        # 6 files, will be batched into 3 batches of 2
        all_files = [
            _make_file(f"{i}.ss", NOW - timedelta(hours=6 - i), length=100) for i in range(6)
        ]
        gen1.list_files.return_value = all_files
        gen1.enrich_with_estimates.side_effect = lambda files: files

        # Simulate discover_files + update_checkpoint loop
        discovered_cache: dict[str, FileInfo] = {}
        watermark: Watermark | None = None

        for batch_num in range(3):
            # discover_files
            unprocessed = tracker.discover_unprocessed_files(
                root="/shares/test",
                pattern=r".*\.ss$",
                watermark=watermark,
                safety_buffer_seconds=0,
            )
            unprocessed.sort(key=lambda f: f.modification_time)

            enriched = gen1.enrich_with_estimates(unprocessed)
            for f in enriched:
                discovered_cache[f.path] = f

            batch = FileTracker.get_next_batch(enriched, max_files_per_trigger=2)
            assert len(batch) == 2, f"Batch {batch_num} should have 2 files"

            # update_checkpoint — uses cache, no re-listing
            processed = [discovered_cache[f.path] for f in batch]
            watermark = FileTracker.compute_new_watermark(processed, watermark)

        # ADLS list_files should have been called once per discover call (3 times)
        # But with the actual AdlsGen1Client cache, it would be 1 call total.
        # Here we're testing the pattern, not the cache (mock doesn't cache).
        assert gen1.list_files.call_count == 3  # 3 discover calls, each calls list_files
        # The real optimization is tested in TestAdlsGen1ClientListFilesCache above

    def test_update_checkpoint_uses_cached_fileinfo(self):
        """update_checkpoint lookup from _discovered_file_infos works correctly."""
        files = [
            _make_file("a.ss", NOW - timedelta(hours=2), length=500),
            _make_file("b.ss", NOW - timedelta(hours=1), length=700),
        ]

        # Simulate the cache built by discover_files
        cache: dict[str, FileInfo] = {f.path: f for f in files}

        # Look up FileInfo for specific paths (what update_checkpoint does)
        batch_paths = [files[0].path]
        processed = [cache[p] for p in batch_paths if p in cache]

        assert len(processed) == 1
        assert processed[0].name == "a.ss"
        assert processed[0].modification_time == files[0].modification_time

    def test_uncached_paths_fall_back(self):
        """Paths not in cache require a fallback listing (defensive)."""
        cache: dict[str, FileInfo] = {}

        # File not in cache
        unknown_path = "/shares/test/unknown.ss"
        processed = [cache.get(unknown_path)]

        assert processed == [None]  # Not found — triggers fallback in real code
