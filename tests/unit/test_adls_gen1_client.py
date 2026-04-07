"""Unit tests for adls_gen1_client — file listing with mocked azure.datalake.store."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

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
