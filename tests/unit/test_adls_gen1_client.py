"""Unit tests for adls_gen1_client — file listing with mocked azure.datalake.store."""

from __future__ import annotations

from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

from dbt.adapters.scope.adls_gen1_client import AdlsGen1Client, FileInfo


class TestFileInfo:
    def test_from_adls_entry_file(self):
        entry = {
            "name": "/shares/test/data/file.ss",
            "type": "FILE",
            "length": 84125,
            "modificationTime": 1775018672000,
        }
        info = FileInfo.from_adls_entry(entry)
        assert info is not None
        assert info.path == "/shares/test/data/file.ss"
        assert info.name == "file.ss"
        assert info.length == 84125
        assert info.modification_time.year == 2026

    def test_from_adls_entry_normalizes_leading_slash(self):
        """ADLS Gen1 SDK returns paths without leading / — we normalize."""
        entry = {
            "name": "local/mdrrahman/ss/file.ss",
            "type": "FILE",
            "length": 100,
            "modificationTime": 1775018672000,
        }
        info = FileInfo.from_adls_entry(entry)
        assert info is not None
        assert info.path == "/local/mdrrahman/ss/file.ss"
        assert info.name == "file.ss"

    def test_from_adls_entry_directory_returns_none(self):
        entry = {"name": "/shares/test/data", "type": "DIRECTORY"}
        assert FileInfo.from_adls_entry(entry) is None

    def test_from_adls_entry_no_mod_time_returns_none(self):
        entry = {"name": "/shares/test/file.ss", "type": "FILE"}
        assert FileInfo.from_adls_entry(entry) is None


class TestAdlsGen1Client:
    @patch("dbt.adapters.scope.adls_gen1_client.adls_core")
    @patch("dbt.adapters.scope.adls_gen1_client.AzureCliCredential")
    def test_list_files_basic(self, mock_cred, mock_adls):
        mock_fs = MagicMock()
        mock_fs.ls.return_value = [
            {
                "name": "/shares/test/a.ss",
                "type": "FILE",
                "length": 100,
                "modificationTime": 1775018672000,
            },
            {
                "name": "/shares/test/b.ss",
                "type": "FILE",
                "length": 200,
                "modificationTime": 1775019672000,
            },
        ]
        mock_adls.AzureDLFileSystem.return_value = mock_fs

        client = AdlsGen1Client("test-account")
        files = client.list_files("/shares/test", recursive=False)
        assert len(files) == 2
        assert files[0].name == "a.ss"

    @patch("dbt.adapters.scope.adls_gen1_client.adls_core")
    @patch("dbt.adapters.scope.adls_gen1_client.AzureCliCredential")
    def test_list_files_with_pattern(self, mock_cred, mock_adls):
        mock_fs = MagicMock()
        mock_fs.ls.return_value = [
            {
                "name": "/shares/test/file.ss",
                "type": "FILE",
                "length": 100,
                "modificationTime": 1775018672000,
            },
            {
                "name": "/shares/test/file.schema",
                "type": "FILE",
                "length": 50,
                "modificationTime": 1775018672000,
            },
        ]
        mock_adls.AzureDLFileSystem.return_value = mock_fs

        client = AdlsGen1Client("test-account")
        files = client.list_files("/shares/test", pattern=r".*\.ss$", recursive=False)
        assert len(files) == 1
        assert files[0].name == "file.ss"

    @patch("dbt.adapters.scope.adls_gen1_client.adls_core")
    @patch("dbt.adapters.scope.adls_gen1_client.AzureCliCredential")
    def test_list_files_sorted_by_modification_time(self, mock_cred, mock_adls):
        mock_fs = MagicMock()
        mock_fs.ls.return_value = [
            {
                "name": "/shares/test/newer.ss",
                "type": "FILE",
                "length": 100,
                "modificationTime": 1775020000000,
            },
            {
                "name": "/shares/test/older.ss",
                "type": "FILE",
                "length": 100,
                "modificationTime": 1775010000000,
            },
        ]
        mock_adls.AzureDLFileSystem.return_value = mock_fs

        client = AdlsGen1Client("test-account")
        files = client.list_files("/shares/test", recursive=False)
        assert files[0].name == "older.ss"
        assert files[1].name == "newer.ss"

    @patch("dbt.adapters.scope.adls_gen1_client.adls_core")
    @patch("dbt.adapters.scope.adls_gen1_client.AzureCliCredential")
    def test_list_files_not_found_returns_empty(self, mock_cred, mock_adls):
        mock_fs = MagicMock()
        mock_fs.ls.side_effect = FileNotFoundError("not found")
        mock_adls.AzureDLFileSystem.return_value = mock_fs

        client = AdlsGen1Client("test-account")
        files = client.list_files("/shares/nonexistent", recursive=False)
        assert files == []

    @patch("dbt.adapters.scope.adls_gen1_client.adls_core")
    @patch("dbt.adapters.scope.adls_gen1_client.AzureCliCredential")
    def test_list_files_recursive(self, mock_cred, mock_adls):
        mock_fs = MagicMock()

        # Root has a directory and a file
        root_entries = [
            {"name": "/shares/test/subdir", "type": "DIRECTORY"},
            {
                "name": "/shares/test/root.ss",
                "type": "FILE",
                "length": 100,
                "modificationTime": 1775018672000,
            },
        ]
        # Subdir has a file
        subdir_entries = [
            {
                "name": "/shares/test/subdir/nested.ss",
                "type": "FILE",
                "length": 200,
                "modificationTime": 1775019672000,
            },
        ]

        mock_fs.ls.side_effect = [root_entries, subdir_entries]
        mock_adls.AzureDLFileSystem.return_value = mock_fs

        client = AdlsGen1Client("test-account")
        files = client.list_files("/shares/test", recursive=True)
        assert len(files) == 2
        names = {f.name for f in files}
        assert names == {"root.ss", "nested.ss"}

    @patch("dbt.adapters.scope.adls_gen1_client.adls_core")
    @patch("dbt.adapters.scope.adls_gen1_client.AzureCliCredential")
    def test_recursive_parallel_multiple_subdirs(self, mock_cred, mock_adls):
        """Multiple sibling directories are walked in parallel."""
        mock_fs = MagicMock()

        root_entries = [
            {"name": "/shares/test/dir_a", "type": "DIRECTORY"},
            {"name": "/shares/test/dir_b", "type": "DIRECTORY"},
            {"name": "/shares/test/dir_c", "type": "DIRECTORY"},
        ]
        dir_a_entries = [
            {
                "name": "/shares/test/dir_a/a.ss",
                "type": "FILE",
                "length": 100,
                "modificationTime": 1775018672000,
            },
        ]
        dir_b_entries = [
            {
                "name": "/shares/test/dir_b/b.ss",
                "type": "FILE",
                "length": 200,
                "modificationTime": 1775019672000,
            },
        ]
        dir_c_entries = [
            {
                "name": "/shares/test/dir_c/c.ss",
                "type": "FILE",
                "length": 300,
                "modificationTime": 1775020672000,
            },
        ]

        def mock_ls(path, detail=True):
            return {
                "/shares/test": root_entries,
                "/shares/test/dir_a": dir_a_entries,
                "/shares/test/dir_b": dir_b_entries,
                "/shares/test/dir_c": dir_c_entries,
            }[path]

        mock_fs.ls.side_effect = mock_ls
        mock_adls.AzureDLFileSystem.return_value = mock_fs

        client = AdlsGen1Client("test-account")
        files = client.list_files("/shares/test", recursive=True)
        assert len(files) == 3
        assert {f.name for f in files} == {"a.ss", "b.ss", "c.ss"}

    @patch("dbt.adapters.scope.adls_gen1_client.adls_core")
    @patch("dbt.adapters.scope.adls_gen1_client.AzureCliCredential")
    def test_recursive_subdir_error_skips_gracefully(self, mock_cred, mock_adls):
        """If one subdirectory fails, the others still succeed."""
        mock_fs = MagicMock()

        root_entries = [
            {"name": "/shares/test/good_dir", "type": "DIRECTORY"},
            {"name": "/shares/test/bad_dir", "type": "DIRECTORY"},
        ]
        good_dir_entries = [
            {
                "name": "/shares/test/good_dir/ok.ss",
                "type": "FILE",
                "length": 100,
                "modificationTime": 1775018672000,
            },
        ]

        def mock_ls(path, detail=True):
            if path == "/shares/test/bad_dir":
                raise FileNotFoundError("gone")
            return {
                "/shares/test": root_entries,
                "/shares/test/good_dir": good_dir_entries,
            }[path]

        mock_fs.ls.side_effect = mock_ls
        mock_adls.AzureDLFileSystem.return_value = mock_fs

        client = AdlsGen1Client("test-account")
        files = client.list_files("/shares/test", recursive=True)
        assert len(files) == 1
        assert files[0].name == "ok.ss"

    @patch("dbt.adapters.scope.adls_gen1_client.adls_core")
    @patch("dbt.adapters.scope.adls_gen1_client.AzureCliCredential")
    def test_recursive_deep_nesting(self, mock_cred, mock_adls):
        """Parallel walk works with nested subdirectories (depth > 1)."""
        mock_fs = MagicMock()

        def mock_ls(path, detail=True):
            return {
                "/root": [{"name": "/root/l1", "type": "DIRECTORY"}],
                "/root/l1": [{"name": "/root/l1/l2", "type": "DIRECTORY"}],
                "/root/l1/l2": [
                    {
                        "name": "/root/l1/l2/deep.ss",
                        "type": "FILE",
                        "length": 50,
                        "modificationTime": 1775018672000,
                    },
                ],
            }[path]

        mock_fs.ls.side_effect = mock_ls
        mock_adls.AzureDLFileSystem.return_value = mock_fs

        client = AdlsGen1Client("test-account")
        files = client.list_files("/root", recursive=True)
        assert len(files) == 1
        assert files[0].name == "deep.ss"


class TestWalkProgressLogging:
    @patch("dbt.adapters.scope.adls_gen1_client.log")
    @patch("dbt.adapters.scope.adls_gen1_client.adls_core")
    @patch("dbt.adapters.scope.adls_gen1_client.AzureCliCredential")
    def test_logs_per_directory_progress(self, mock_cred, mock_adls, mock_log):
        """Walk logs depth, dir/file counts, and in-flight per directory."""
        mock_fs = MagicMock()

        def mock_ls(path, detail=True):
            return {
                "/root": [
                    {"name": "/root/d1", "type": "DIRECTORY"},
                    {"name": "/root/d2", "type": "DIRECTORY"},
                ],
                "/root/d1": [
                    {
                        "name": "/root/d1/a.ss",
                        "type": "FILE",
                        "length": 100,
                        "modificationTime": 1775018672000,
                    },
                ],
                "/root/d2": [
                    {
                        "name": "/root/d2/b.ss",
                        "type": "FILE",
                        "length": 100,
                        "modificationTime": 1775019672000,
                    },
                ],
            }[path]

        mock_fs.ls.side_effect = mock_ls
        mock_adls.AzureDLFileSystem.return_value = mock_fs

        client = AdlsGen1Client("test-account")
        client.list_files("/root", recursive=True)

        debug_msgs = [c.args[0] if c.args else "" for c in mock_log.debug.call_args_list]
        assert any("Depth" in m and "dirs" in m and "files" in m for m in debug_msgs)
        assert any("Walk complete:" in m and "directories scanned" in m for m in debug_msgs)

    @patch("dbt.adapters.scope.adls_gen1_client.log")
    @patch("dbt.adapters.scope.adls_gen1_client.adls_core")
    @patch("dbt.adapters.scope.adls_gen1_client.AzureCliCredential")
    def test_logs_timing_for_non_recursive(self, mock_cred, mock_adls, mock_log):
        mock_fs = MagicMock()
        mock_fs.ls.return_value = [
            {
                "name": "/shares/test/a.ss",
                "type": "FILE",
                "length": 100,
                "modificationTime": 1775018672000,
            },
        ]
        mock_adls.AzureDLFileSystem.return_value = mock_fs

        client = AdlsGen1Client("test-account")
        client.list_files("/shares/test", recursive=False)

        debug_msgs = [c.args[0] if c.args else "" for c in mock_log.debug.call_args_list]
        assert any("ls" in m and "completed in" in m for m in debug_msgs)
        assert any("Total walk of" in m for m in debug_msgs)

    @patch("dbt.adapters.scope.adls_gen1_client.log")
    @patch("dbt.adapters.scope.adls_gen1_client.adls_core")
    @patch("dbt.adapters.scope.adls_gen1_client.AzureCliCredential")
    def test_logs_on_not_found(self, mock_cred, mock_adls, mock_log):
        """Walk complete is logged even when root path not found."""
        mock_fs = MagicMock()
        mock_fs.ls.side_effect = FileNotFoundError("not found")
        mock_adls.AzureDLFileSystem.return_value = mock_fs

        client = AdlsGen1Client("test-account")
        client.list_files("/shares/gone", recursive=True)

        debug_msgs = [c.args[0] if c.args else "" for c in mock_log.debug.call_args_list]
        assert any("Walk complete:" in m for m in debug_msgs)


class TestEstimateBytes:
    """Tests for estimate_bytes — SSv3/v4 vs SSv5/v6 detection."""

    @patch("dbt.adapters.scope.adls_gen1_client.adls_core")
    @patch("dbt.adapters.scope.adls_gen1_client.AzureCliCredential")
    def test_ssv3_no_sibling_folder(self, mock_cred, mock_adls):
        """SSv3/v4: no sibling folder → returns (file_length, [])."""
        mock_fs = MagicMock()
        mock_fs.info.side_effect = FileNotFoundError("not found")
        mock_adls.AzureDLFileSystem.return_value = mock_fs

        client = AdlsGen1Client("test-account")
        est_bytes, contrib = client.estimate_bytes("/shares/test/data.ss", 727393)

        assert est_bytes == 727393
        assert contrib == []

    @patch("dbt.adapters.scope.adls_gen1_client.adls_core")
    @patch("dbt.adapters.scope.adls_gen1_client.AzureCliCredential")
    def test_ssv5_with_du_files(self, mock_cred, mock_adls):
        """SSv5/v6: sibling folder with .du files → returns sum."""
        mock_fs = MagicMock()
        mock_fs.info.return_value = {"type": "DIRECTORY"}
        mock_fs.ls.return_value = [
            {"name": "/shares/test/data/part-00000.du", "type": "FILE", "length": 50_000_000},
            {"name": "/shares/test/data/part-00001.du", "type": "FILE", "length": 48_000_000},
        ]
        mock_adls.AzureDLFileSystem.return_value = mock_fs

        client = AdlsGen1Client("test-account")
        est_bytes, contrib = client.estimate_bytes("/shares/test/data.ss", 4096)

        assert est_bytes == 4096 + 50_000_000 + 48_000_000
        assert len(contrib) == 2
        assert "/shares/test/data/part-00000.du" in contrib
        assert "/shares/test/data/part-00001.du" in contrib

    @patch("dbt.adapters.scope.adls_gen1_client.adls_core")
    @patch("dbt.adapters.scope.adls_gen1_client.AzureCliCredential")
    def test_ssv5_with_delta_subfolder(self, mock_cred, mock_adls):
        """SSv5 with delta updates — recursive listing includes subdirs."""
        mock_fs = MagicMock()
        mock_fs.info.return_value = {"type": "DIRECTORY"}

        def mock_ls(path, detail=True):
            return {
                "/shares/test/data": [
                    {
                        "name": "/shares/test/data/part-00000.du",
                        "type": "FILE",
                        "length": 50_000_000,
                    },
                    {"name": "/shares/test/data/delta-0", "type": "DIRECTORY"},
                ],
                "/shares/test/data/delta-0": [
                    {
                        "name": "/shares/test/data/delta-0/part-00000.du",
                        "type": "FILE",
                        "length": 1_000_000,
                    },
                ],
            }[path]

        mock_fs.ls.side_effect = mock_ls
        mock_adls.AzureDLFileSystem.return_value = mock_fs

        client = AdlsGen1Client("test-account")
        est_bytes, contrib = client.estimate_bytes("/shares/test/data.ss", 4096)

        assert est_bytes == 4096 + 50_000_000 + 1_000_000
        assert len(contrib) == 2

    @patch("dbt.adapters.scope.adls_gen1_client.adls_core")
    @patch("dbt.adapters.scope.adls_gen1_client.AzureCliCredential")
    def test_empty_sibling_folder(self, mock_cred, mock_adls):
        """Empty sibling folder → returns (manifest_size, [])."""
        mock_fs = MagicMock()
        mock_fs.info.return_value = {"type": "DIRECTORY"}
        mock_fs.ls.return_value = []
        mock_adls.AzureDLFileSystem.return_value = mock_fs

        client = AdlsGen1Client("test-account")
        est_bytes, contrib = client.estimate_bytes("/shares/test/data.ss", 2048)

        assert est_bytes == 2048
        assert contrib == []

    def test_invalid_extension_raises_valueerror(self):
        client = AdlsGen1Client.__new__(AdlsGen1Client)
        with pytest.raises(ValueError, match=r"must end with '\.ss'"):
            client.estimate_bytes("/shares/test/data.parquet", 100)


class TestEnrichWithEstimates:
    """Tests for enrich_with_estimates — bulk enrichment of FileInfo lists."""

    @patch("dbt.adapters.scope.adls_gen1_client.adls_core")
    @patch("dbt.adapters.scope.adls_gen1_client.AzureCliCredential")
    def test_enriches_all_files(self, mock_cred, mock_adls):
        mock_fs = MagicMock()
        # No sibling folders (SSv3/v4 for both)
        mock_fs.info.side_effect = FileNotFoundError("not found")
        mock_adls.AzureDLFileSystem.return_value = mock_fs

        client = AdlsGen1Client("test-account")
        files = [
            FileInfo(
                path="/shares/test/a.ss",
                name="a.ss",
                length=1000,
                modification_time=datetime(2026, 4, 1, tzinfo=timezone.utc),
            ),
            FileInfo(
                path="/shares/test/b.ss",
                name="b.ss",
                length=2000,
                modification_time=datetime(2026, 4, 2, tzinfo=timezone.utc),
            ),
        ]

        enriched = client.enrich_with_estimates(files)
        assert len(enriched) == 2
        assert enriched[0].estimated_bytes == 1000
        assert enriched[0].contributing_files == ()
        assert enriched[1].estimated_bytes == 2000
        assert enriched[1].contributing_files == ()

    @patch("dbt.adapters.scope.adls_gen1_client.adls_core")
    @patch("dbt.adapters.scope.adls_gen1_client.AzureCliCredential")
    def test_enriches_ssv5_files(self, mock_cred, mock_adls):
        mock_fs = MagicMock()
        mock_fs.info.return_value = {"type": "DIRECTORY"}
        mock_fs.ls.return_value = [
            {"name": "/shares/test/a/part-00000.du", "type": "FILE", "length": 5_000_000},
        ]
        mock_adls.AzureDLFileSystem.return_value = mock_fs

        client = AdlsGen1Client("test-account")
        files = [
            FileInfo(
                path="/shares/test/a.ss",
                name="a.ss",
                length=4096,
                modification_time=datetime(2026, 4, 1, tzinfo=timezone.utc),
            ),
        ]

        enriched = client.enrich_with_estimates(files)
        assert enriched[0].estimated_bytes == 4096 + 5_000_000
        assert enriched[0].contributing_files == ("/shares/test/a/part-00000.du",)

    def test_empty_list_returns_empty(self):
        client = AdlsGen1Client.__new__(AdlsGen1Client)
        assert client.enrich_with_estimates([]) == []

    @patch("dbt.adapters.scope.adls_gen1_client.adls_core")
    @patch("dbt.adapters.scope.adls_gen1_client.AzureCliCredential")
    def test_fallback_on_error(self, mock_cred, mock_adls):
        """If estimate_bytes fails for a file, fall back to file length."""
        mock_fs = MagicMock()
        mock_fs.info.side_effect = Exception("network error")
        mock_adls.AzureDLFileSystem.return_value = mock_fs

        client = AdlsGen1Client("test-account")
        files = [
            FileInfo(
                path="/shares/test/bad.ss",
                name="bad.ss",
                length=999,
                modification_time=datetime(2026, 4, 1, tzinfo=timezone.utc),
            ),
        ]

        enriched = client.enrich_with_estimates(files)
        assert enriched[0].estimated_bytes == 999
        assert enriched[0].contributing_files == ()
