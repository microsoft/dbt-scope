"""Unit tests for checkpoint — watermark + sources with mocked ADLS."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
from azure.core.exceptions import ResourceNotFoundError

from dbt.adapters.scope.checkpoint import CheckpointManager, PendingBatch, Watermark


def _pending(watermark_before: Watermark | None = None) -> PendingBatch:
    return PendingBatch(
        token="tok-1",
        batch_id=4,
        run_id="run-1",
        job_name="events_incremental_batch_1_of_1_files_2",
        files=[("/shares/a.ss", 1775018672000), ("/shares/b.ss", 1775018673500)],
        watermark_before=watermark_before,
        delta_version_before=12,
    )


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

    def test_watermark_without_delta_version_loads(self):
        raw = json.dumps({"version": 4, "modifiedTime": "2026-04-01T00:00:00+00:00", "batchId": 4})
        wm = Watermark.from_json(raw)
        assert wm.delta_version is None
        assert "deltaVersion" not in json.loads(wm.to_json())

    def test_delta_version_roundtrip(self):
        wm = Watermark(version=1, modified_time="2026-04-01T00:00:00+00:00", batch_id=1)
        wm.delta_version = 42
        assert json.loads(wm.to_json())["deltaVersion"] == 42
        assert Watermark.from_json(wm.to_json()).delta_version == 42

    def test_same_position_ignores_delta_version(self):
        wm = Watermark(version=1, modified_time="2026-04-01T00:00:00+00:00", batch_id=1)
        assert wm.same_position(Watermark(1, "2026-04-01T00:00:00+00:00", 1, delta_version=9))
        assert not wm.same_position(Watermark(2, "2026-04-02T00:00:00+00:00", 2))
        assert not wm.same_position(None)


class TestPendingBatch:
    def test_json_roundtrip(self):
        pending = _pending(Watermark(3, "2026-04-01T00:00:00+00:00", 3, delta_version=12))
        data = json.loads(pending.to_json())
        assert data["schemaVersion"] == 1
        assert data["token"] == "tok-1"
        assert data["deltaVersionBefore"] == 12
        assert data["files"][0] == {"path": "/shares/a.ss", "modificationTime": 1775018672000}
        assert PendingBatch.from_json(pending.to_json()) == pending

    def test_applies_to_the_watermark_it_was_discovered_from(self):
        before = Watermark(3, "2026-04-01T00:00:00+00:00", 3)
        pending = _pending(before)
        assert pending.applies_to(Watermark(3, "2026-04-01T00:00:00+00:00", 3, delta_version=7))
        assert not pending.applies_to(Watermark(4, "2026-04-02T00:00:00+00:00", 4))
        assert not pending.applies_to(None)

    def test_first_batch_applies_only_without_watermark(self):
        pending = _pending(None)
        assert pending.applies_to(None)
        assert not pending.applies_to(Watermark(0, "2026-04-01T00:00:00+00:00", 0))

    def test_modification_times_are_utc(self):
        assert _pending().modification_times == [
            datetime(2026, 4, 1, 4, 44, 32, tzinfo=timezone.utc),
            datetime(2026, 4, 1, 4, 44, 33, 500000, tzinfo=timezone.utc),
        ]
        assert _pending().file_paths == ["/shares/a.ss", "/shares/b.ss"]


class TestCheckpointManagerWatermark:
    @patch("dbt.adapters.scope.checkpoint.DataLakeServiceClient")
    def test_read_watermark(self, mock_service):
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

        result = CheckpointManager(credential=MagicMock()).read_watermark(
            "abfss://c@a.dfs.core.windows.net/d/t"
        )
        assert result is not None
        assert result.batch_id == 3

    @patch("dbt.adapters.scope.checkpoint.DataLakeServiceClient")
    def test_read_watermark_none_on_missing(self, mock_service):
        mock_fs = MagicMock()
        mock_fs.get_file_client.side_effect = Exception("Not found")
        mock_service.return_value.get_file_system_client.return_value = mock_fs
        assert (
            CheckpointManager(credential=MagicMock()).read_watermark(
                "abfss://c@a.dfs.core.windows.net/d/t"
            )
            is None
        )

    @patch("dbt.adapters.scope.checkpoint.DataLakeServiceClient")
    def test_read_watermark_propagates_credential_exhaustion(self, mock_service):
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
            CheckpointManager(credential=MagicMock()).read_watermark(
                "abfss://c@a.dfs.core.windows.net/d/t"
            )

    def test_read_watermark_none_for_bad_path(self):
        assert CheckpointManager(credential=MagicMock()).read_watermark("https://bad") is None

    @patch("dbt.adapters.scope.checkpoint.DataLakeServiceClient")
    def test_write_watermark(self, mock_service):
        mock_dir = MagicMock()
        mock_file = MagicMock()
        mock_fs = MagicMock()
        mock_fs.get_directory_client.return_value = mock_dir
        mock_fs.get_file_client.return_value = mock_file
        mock_service.return_value.get_file_system_client.return_value = mock_fs

        wm = Watermark(version=0, modified_time="2026-04-01T12:00:00+00:00", batch_id=0)
        CheckpointManager(credential=MagicMock()).write_watermark(
            "abfss://c@a.dfs.core.windows.net/d/t", wm
        )
        mock_file.upload_data.assert_called_once()

    @patch("dbt.adapters.scope.checkpoint.DataLakeServiceClient")
    def test_delete_watermark(self, mock_service):
        mock_file = MagicMock()
        mock_fs = MagicMock()
        mock_fs.get_file_client.return_value = mock_file
        mock_fs.get_paths.return_value = []
        mock_service.return_value.get_file_system_client.return_value = mock_fs

        CheckpointManager(credential=MagicMock()).delete_watermark(
            "abfss://c@a.dfs.core.windows.net/d/t"
        )
        deleted = [c.args[0] for c in mock_fs.get_file_client.call_args_list]
        assert deleted == ["d/t/_checkpoint/watermark.json", "d/t/_checkpoint/pending.json"]
        assert mock_file.delete_file.call_count == 2

    @patch("dbt.adapters.scope.checkpoint.DataLakeServiceClient")
    def test_delete_watermark_fails_if_pending_cannot_be_deleted(self, mock_service):
        mock_watermark = MagicMock()
        mock_pending = MagicMock()
        mock_pending.delete_file.side_effect = OSError("throttled")
        mock_fs = MagicMock()
        mock_fs.get_file_client.side_effect = [mock_watermark, mock_pending]
        mock_service.return_value.get_file_system_client.return_value = mock_fs

        with pytest.raises(OSError, match="throttled"):
            CheckpointManager(credential=MagicMock()).delete_watermark(
                "abfss://c@a.dfs.core.windows.net/d/t"
            )


class TestCheckpointManagerPending:
    @patch("dbt.adapters.scope.checkpoint.DataLakeServiceClient")
    def test_unreadable_pending_is_ignored(self, mock_service):
        mock_file = MagicMock()
        mock_file.download_file.return_value.readall.return_value = b"{not json"
        mock_service.return_value.get_file_system_client.return_value.get_file_client.return_value = mock_file

        assert (
            CheckpointManager(credential=MagicMock()).read_pending(
                "abfss://c@a.dfs.core.windows.net/d/t"
            )
            is None
        )

    @patch("dbt.adapters.scope.checkpoint.DataLakeServiceClient")
    def test_write_pending_failure_propagates(self, mock_service):
        mock_file = MagicMock()
        mock_file.upload_data.side_effect = OSError("forbidden")
        mock_service.return_value.get_file_system_client.return_value.get_file_client.return_value = mock_file

        with pytest.raises(OSError, match="forbidden"):
            CheckpointManager(credential=MagicMock()).write_pending(
                "abfss://c@a.dfs.core.windows.net/d/t", _pending()
            )

    @patch("dbt.adapters.scope.checkpoint.DataLakeServiceClient")
    def test_delete_pending_failure_only_warns(self, mock_service):
        mock_file = MagicMock()
        mock_file.delete_file.side_effect = OSError("throttled")
        mock_service.return_value.get_file_system_client.return_value.get_file_client.return_value = mock_file

        CheckpointManager(credential=MagicMock()).delete_pending(
            "abfss://c@a.dfs.core.windows.net/d/t"
        )

    @patch("dbt.adapters.scope.checkpoint.DataLakeServiceClient")
    def test_delete_pending_absent_is_noop(self, mock_service):
        mock_file = MagicMock()
        mock_file.delete_file.side_effect = ResourceNotFoundError("missing")
        mock_service.return_value.get_file_system_client.return_value.get_file_client.return_value = mock_file

        CheckpointManager(credential=MagicMock()).delete_pending(
            "abfss://c@a.dfs.core.windows.net/d/t", strict=True
        )


class TestCheckpointManagerSources:
    @patch("dbt.adapters.scope.checkpoint.DataLakeServiceClient")
    def test_write_jsonl_on_normal_batch(self, mock_service):
        """Non-compaction batch writes a JSONL diff file."""
        from datetime import datetime, timezone

        mock_dir = MagicMock()
        mock_file = MagicMock()
        mock_fs = MagicMock()
        mock_fs.get_directory_client.return_value = mock_dir
        mock_fs.get_file_client.return_value = mock_file
        mock_service.return_value.get_file_system_client.return_value = mock_fs

        CheckpointManager(credential=MagicMock()).write_batch_sources(
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
    def test_batch_zero_always_writes_jsonl(self, mock_service):
        """Batch 0 is never a compaction boundary, even with interval=1."""
        from datetime import datetime, timezone

        mock_dir = MagicMock()
        mock_file = MagicMock()
        mock_fs = MagicMock()
        mock_fs.get_directory_client.return_value = mock_dir
        mock_fs.get_file_client.return_value = mock_file
        mock_service.return_value.get_file_system_client.return_value = mock_fs

        CheckpointManager(credential=MagicMock()).write_batch_sources(
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
    def test_cleanup_deletes_oldest(self, mock_service):
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

        deleted = CheckpointManager(credential=MagicMock()).cleanup_sources(
            "abfss://c@a.dfs.core.windows.net/d/t", max_files=3
        )
        assert deleted == 2
        assert mock_file.delete_file.call_count == 2

    @patch("dbt.adapters.scope.checkpoint.DataLakeServiceClient")
    def test_cleanup_noop_under_limit(self, mock_service):
        mock_fs = MagicMock()
        mock_fs.get_paths.return_value = [
            SimpleNamespace(name="d/t/_checkpoint/sources/0", is_directory=False),
        ]
        mock_service.return_value.get_file_system_client.return_value = mock_fs

        deleted = CheckpointManager(credential=MagicMock()).cleanup_sources(
            "abfss://c@a.dfs.core.windows.net/d/t", max_files=100
        )
        assert deleted == 0
