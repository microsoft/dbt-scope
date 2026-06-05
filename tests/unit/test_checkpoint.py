"""Unit tests for checkpoint — watermark + sources with mocked ADLS."""

from __future__ import annotations

import json
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

from dbt.adapters.scope.checkpoint import CheckpointManager, Watermark


class TestWatermark:
    def test_to_json_roundtrip(self):
        wm = Watermark(version=3, modified_time="2026-04-01T12:00:00+00:00", batch_id=7)
        raw = wm.to_json()
        parsed = Watermark.from_json(raw)
        assert parsed.version == 3
        assert parsed.modified_time == "2026-04-01T12:00:00+00:00"
        assert parsed.batch_id == 7

    def test_from_json_minimal(self):
        raw = json.dumps({"version": 0, "modifiedTime": "", "batchId": 0})
        wm = Watermark.from_json(raw)
        assert wm.version == 0
        assert wm.batch_id == 0

    def test_modified_time_dt_parses(self):
        wm = Watermark(modified_time="2026-04-01T12:34:56.789000+00:00")
        dt = wm.modified_time_dt
        assert dt is not None
        assert dt.year == 2026

    def test_modified_time_dt_none_for_empty(self):
        wm = Watermark(modified_time="")
        assert wm.modified_time_dt is None

    def test_to_json_has_batch_id_no_files_processed(self):
        wm = Watermark(version=1, modified_time="2026-04-01T00:00:00+00:00", batch_id=5)
        data = json.loads(wm.to_json())
        assert data["batchId"] == 5
        assert "filesProcessed" not in data

    def test_backward_compat_old_format(self):
        """Old watermark with filesProcessed should still parse."""
        raw = json.dumps(
            {
                "version": 2,
                "modifiedTime": "2026-04-01T00:00:00+00:00",
                "filesProcessed": 50,
            }
        )
        wm = Watermark.from_json(raw)
        assert wm.version == 2
        assert wm.batch_id == 0


class TestCheckpointManagerWatermark:
    @patch("dbt.adapters.scope.checkpoint.DataLakeServiceClient")
    @patch("dbt.adapters.scope.checkpoint.AzureCliCredential")
    def test_read_watermark(self, mock_cred, mock_service):
        wm_json = Watermark(
            version=1, modified_time="2026-04-01T12:00:00+00:00", batch_id=3
        ).to_json()
        mock_download = MagicMock()
        mock_download.readall.return_value = wm_json.encode("utf-8")
        mock_file = MagicMock()
        mock_file.download_file.return_value = mock_download
        mock_fs = MagicMock()
        mock_fs.get_file_client.return_value = mock_file
        mock_service.return_value.get_file_system_client.return_value = mock_fs

        result = CheckpointManager().read_watermark("abfss://c@a.dfs.core.windows.net/d/t")
        assert result is not None
        assert result.batch_id == 3

    @patch("dbt.adapters.scope.checkpoint.DataLakeServiceClient")
    @patch("dbt.adapters.scope.checkpoint.AzureCliCredential")
    def test_read_watermark_none_on_missing(self, mock_cred, mock_service):
        mock_fs = MagicMock()
        mock_fs.get_file_client.side_effect = Exception("Not found")
        mock_service.return_value.get_file_system_client.return_value = mock_fs
        assert CheckpointManager().read_watermark("abfss://c@a.dfs.core.windows.net/d/t") is None

    @patch("dbt.adapters.scope.checkpoint.DataLakeServiceClient")
    @patch("dbt.adapters.scope.checkpoint.AzureCliCredential")
    def test_read_watermark_propagates_credential_exhaustion(self, mock_cred, mock_service):
        """``read_watermark`` MUST NOT swallow ``CredentialUnavailableError``.

        Returning ``None`` on auth failure would silently flip an
        incremental run into a full refresh and re-ingest the entire
        source history. Regression for PR #32.
        """
        import pytest
        from azure.identity import CredentialUnavailableError

        mock_fs = MagicMock()
        mock_fs.get_file_client.side_effect = CredentialUnavailableError(
            message="Failed to invoke the Azure CLI"
        )
        mock_service.return_value.get_file_system_client.return_value = mock_fs

        with pytest.raises(CredentialUnavailableError):
            CheckpointManager().read_watermark("abfss://c@a.dfs.core.windows.net/d/t")

    def test_read_watermark_none_for_bad_path(self):
        assert CheckpointManager().read_watermark("https://bad") is None

    @patch("dbt.adapters.scope.checkpoint.DataLakeServiceClient")
    @patch("dbt.adapters.scope.checkpoint.AzureCliCredential")
    def test_write_watermark(self, mock_cred, mock_service):
        mock_dir = MagicMock()
        mock_file = MagicMock()
        mock_fs = MagicMock()
        mock_fs.get_directory_client.return_value = mock_dir
        mock_fs.get_file_client.return_value = mock_file
        mock_service.return_value.get_file_system_client.return_value = mock_fs

        wm = Watermark(version=0, modified_time="2026-04-01T12:00:00+00:00", batch_id=0)
        CheckpointManager().write_watermark("abfss://c@a.dfs.core.windows.net/d/t", wm)
        mock_file.upload_data.assert_called_once()

    @patch("dbt.adapters.scope.checkpoint.DataLakeServiceClient")
    @patch("dbt.adapters.scope.checkpoint.AzureCliCredential")
    def test_delete_watermark(self, mock_cred, mock_service):
        mock_file = MagicMock()
        mock_fs = MagicMock()
        mock_fs.get_file_client.return_value = mock_file
        mock_fs.get_paths.return_value = []
        mock_service.return_value.get_file_system_client.return_value = mock_fs

        CheckpointManager().delete_watermark("abfss://c@a.dfs.core.windows.net/d/t")
        mock_file.delete_file.assert_called_once()


class TestCheckpointManagerSources:
    @patch("dbt.adapters.scope.checkpoint.DataLakeServiceClient")
    @patch("dbt.adapters.scope.checkpoint.AzureCliCredential")
    def test_write_jsonl_on_normal_batch(self, mock_cred, mock_service):
        """Non-compaction batch writes a JSONL diff file."""
        from datetime import datetime, timezone

        mock_dir = MagicMock()
        mock_file = MagicMock()
        mock_fs = MagicMock()
        mock_fs.get_directory_client.return_value = mock_dir
        mock_fs.get_file_client.return_value = mock_file
        mock_service.return_value.get_file_system_client.return_value = mock_fs

        CheckpointManager().write_batch_sources(
            "abfss://c@a.dfs.core.windows.net/d/t",
            batch_id=3,
            file_paths=["/shares/a.ss", "/shares/b.ss"],
            modification_times=[
                datetime(2026, 4, 1, 12, 0, tzinfo=timezone.utc),
                datetime(2026, 4, 1, 13, 0, tzinfo=timezone.utc),
            ],
            compaction_interval=10,
        )

        mock_file.upload_data.assert_called_once()
        uploaded = mock_file.upload_data.call_args[0][0].decode("utf-8")
        lines = uploaded.strip().split("\n")
        assert len(lines) == 2
        r0 = json.loads(lines[0])
        assert r0["path"] == "/shares/a.ss"
        assert r0["batchId"] == 3
        assert "modificationTime" in r0
        assert "batchProcessingTime" in r0

    @patch("dbt.adapters.scope.checkpoint.DataLakeServiceClient")
    @patch("dbt.adapters.scope.checkpoint.AzureCliCredential")
    def test_batch_zero_always_writes_jsonl(self, mock_cred, mock_service):
        """Batch 0 is never a compaction boundary, even with interval=1."""
        from datetime import datetime, timezone

        mock_dir = MagicMock()
        mock_file = MagicMock()
        mock_fs = MagicMock()
        mock_fs.get_directory_client.return_value = mock_dir
        mock_fs.get_file_client.return_value = mock_file
        mock_service.return_value.get_file_system_client.return_value = mock_fs

        CheckpointManager().write_batch_sources(
            "abfss://c@a.dfs.core.windows.net/d/t",
            batch_id=0,
            file_paths=["/shares/a.ss"],
            modification_times=[datetime(2026, 4, 1, 12, 0, tzinfo=timezone.utc)],
            compaction_interval=1,
        )

        # Should write JSONL (not parquet) for batch 0
        mock_file.upload_data.assert_called_once()
        uploaded = mock_file.upload_data.call_args[0][0]
        assert isinstance(uploaded, bytes)
        # JSONL is text, not parquet binary
        json.loads(uploaded.decode("utf-8").split("\n")[0])

    @patch("dbt.adapters.scope.checkpoint.DataLakeServiceClient")
    @patch("dbt.adapters.scope.checkpoint.AzureCliCredential")
    def test_cleanup_deletes_oldest(self, mock_cred, mock_service):
        mock_file = MagicMock()
        mock_fs = MagicMock()
        mock_fs.get_file_client.return_value = mock_file
        mock_fs.get_paths.return_value = [
            SimpleNamespace(name="d/t/_checkpoint/sources/0", is_directory=False),
            SimpleNamespace(name="d/t/_checkpoint/sources/1", is_directory=False),
            SimpleNamespace(name="d/t/_checkpoint/sources/2", is_directory=False),
            SimpleNamespace(name="d/t/_checkpoint/sources/3", is_directory=False),
            SimpleNamespace(name="d/t/_checkpoint/sources/4", is_directory=False),
        ]
        mock_service.return_value.get_file_system_client.return_value = mock_fs

        deleted = CheckpointManager().cleanup_sources(
            "abfss://c@a.dfs.core.windows.net/d/t", max_files=3
        )
        assert deleted == 2
        assert mock_file.delete_file.call_count == 2

    @patch("dbt.adapters.scope.checkpoint.DataLakeServiceClient")
    @patch("dbt.adapters.scope.checkpoint.AzureCliCredential")
    def test_cleanup_noop_under_limit(self, mock_cred, mock_service):
        mock_fs = MagicMock()
        mock_fs.get_paths.return_value = [
            SimpleNamespace(name="d/t/_checkpoint/sources/0", is_directory=False),
        ]
        mock_service.return_value.get_file_system_client.return_value = mock_fs

        deleted = CheckpointManager().cleanup_sources(
            "abfss://c@a.dfs.core.windows.net/d/t", max_files=100
        )
        assert deleted == 0
