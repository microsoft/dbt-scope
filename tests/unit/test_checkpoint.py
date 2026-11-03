"""Unit tests for checkpoint — watermark + sources with mocked ADLS."""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pytest
from azure.core import MatchConditions
from azure.core.exceptions import (
    ResourceExistsError,
    ResourceModifiedError,
    ResourceNotFoundError,
)

from dbt.adapters.scope.checkpoint import (
    CheckpointManager,
    PendingBatch,
    TableOwner,
    Watermark,
)


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


_TABLE = "abfss://c@a.dfs.core.windows.net/d/t"
_T0 = datetime(2026, 11, 1, 12, 0, tzinfo=timezone.utc)
_OLDER = TableOwner(run_id="older", run_started_at=_T0)
_NEWER = TableOwner(run_id="newer", run_started_at=_T0 + timedelta(hours=1))


class _FakeOwnerFile:
    """In-memory ``owner.json`` that enforces the ETag conditions of ``upload_data``.

    Each entry of ``race`` is written by a competing run right after the next read.
    """

    def __init__(self, content: str | None = None, race: list[str] | None = None) -> None:
        self.content = content
        self.etag = 0
        self.race = list(race or [])
        self.conditions: list[MatchConditions] = []

    def _write(self, content: str) -> None:
        self.content = content
        self.etag += 1

    def download_file(self):
        content, etag = self.content, str(self.etag)
        if self.race:
            self._write(self.race.pop(0))
        if content is None:
            raise ResourceNotFoundError("owner.json not found")
        return SimpleNamespace(
            readall=lambda: content.encode("utf-8"), properties=SimpleNamespace(etag=etag)
        )

    def upload_data(self, data: bytes, *, overwrite: bool, match_condition, etag=None) -> None:
        assert overwrite
        self.conditions.append(match_condition)
        if match_condition is MatchConditions.IfMissing and self.content is not None:
            raise ResourceExistsError("owner.json exists")
        if match_condition is MatchConditions.IfNotModified and etag != str(self.etag):
            raise ResourceModifiedError("owner.json changed")
        self._write(data.decode("utf-8"))


def _owner_manager(mock_service: MagicMock, fake: _FakeOwnerFile) -> CheckpointManager:
    mock_service.return_value.get_file_system_client.return_value.get_file_client.return_value = (
        fake
    )
    return CheckpointManager(credential=MagicMock())


class TestTableOwner:
    def test_later_start_outranks(self):
        assert _NEWER.outranks(_OLDER)
        assert not _OLDER.outranks(_NEWER)

    def test_run_id_breaks_a_start_time_tie(self):
        a = TableOwner(run_id="a", run_started_at=_T0)
        b = TableOwner(run_id="b", run_started_at=_T0)
        assert b.outranks(a)
        assert not a.outranks(b)

    def test_json_roundtrip(self):
        assert TableOwner.from_json(_NEWER.to_json()) == _NEWER

    @pytest.mark.parametrize(
        "raw",
        [
            "",
            "{}",
            "[]",
            json.dumps({"runId": "r", "runStartedAt": "not a time"}),
            json.dumps({"runId": "r", "runStartedAt": "2026-11-01T12:00:00"}),
        ],
    )
    def test_rejects_invalid_records(self, raw):
        with pytest.raises(ValueError):
            TableOwner.from_json(raw)


class TestClaimOwner:
    @patch("dbt.adapters.scope.checkpoint.DataLakeServiceClient")
    def test_first_claim_creates_the_file(self, mock_service):
        fake = _FakeOwnerFile()
        mgr = _owner_manager(mock_service, fake)

        assert mgr.claim_owner(_TABLE, _OLDER) == _OLDER

        fs = mock_service.return_value.get_file_system_client.return_value
        fs.get_file_client.assert_called_with("d/t/_checkpoint/owner.json")
        fs.get_directory_client.assert_called_with("d/t/_checkpoint")
        assert fake.conditions == [MatchConditions.IfMissing]
        assert TableOwner.from_json(fake.content) == _OLDER

    @patch("dbt.adapters.scope.checkpoint.DataLakeServiceClient")
    def test_newer_run_takes_over(self, mock_service):
        fake = _FakeOwnerFile(_OLDER.to_json())

        assert _owner_manager(mock_service, fake).claim_owner(_TABLE, _NEWER) == _NEWER

        assert fake.conditions == [MatchConditions.IfNotModified]
        assert TableOwner.from_json(fake.content) == _NEWER

    @patch("dbt.adapters.scope.checkpoint.DataLakeServiceClient")
    def test_older_run_cannot_take_over(self, mock_service):
        fake = _FakeOwnerFile(_NEWER.to_json())

        assert _owner_manager(mock_service, fake).claim_owner(_TABLE, _OLDER) == _NEWER

        assert fake.conditions == []

    @patch("dbt.adapters.scope.checkpoint.DataLakeServiceClient")
    def test_owner_reclaiming_does_not_write(self, mock_service):
        fake = _FakeOwnerFile(_OLDER.to_json())

        assert _owner_manager(mock_service, fake).claim_owner(_TABLE, _OLDER) == _OLDER

        assert fake.conditions == []

    @patch("dbt.adapters.scope.checkpoint.DataLakeServiceClient")
    def test_loses_a_race_to_a_newer_run(self, mock_service):
        fake = _FakeOwnerFile(race=[_NEWER.to_json()])

        assert _owner_manager(mock_service, fake).claim_owner(_TABLE, _OLDER) == _NEWER

        assert fake.conditions == [MatchConditions.IfMissing]
        assert TableOwner.from_json(fake.content) == _NEWER

    @patch("dbt.adapters.scope.checkpoint.DataLakeServiceClient")
    def test_wins_a_race_against_an_older_run(self, mock_service):
        fake = _FakeOwnerFile(race=[_OLDER.to_json()])

        assert _owner_manager(mock_service, fake).claim_owner(_TABLE, _NEWER) == _NEWER

        assert fake.conditions == [MatchConditions.IfMissing, MatchConditions.IfNotModified]
        assert TableOwner.from_json(fake.content) == _NEWER

    @patch("dbt.adapters.scope.checkpoint.DataLakeServiceClient")
    def test_replaces_an_unreadable_owner(self, mock_service):
        fake = _FakeOwnerFile("")

        assert _owner_manager(mock_service, fake).claim_owner(_TABLE, _OLDER) == _OLDER

        assert fake.conditions == [MatchConditions.IfNotModified]
        assert TableOwner.from_json(fake.content) == _OLDER

    @patch("dbt.adapters.scope.checkpoint.DataLakeServiceClient")
    def test_gives_up_when_the_owner_keeps_changing(self, mock_service):
        fake = _FakeOwnerFile(race=[""] * 5)

        with pytest.raises(RuntimeError, match="changed during each of 5 attempts"):
            _owner_manager(mock_service, fake).claim_owner(_TABLE, _OLDER)

        assert len(fake.conditions) == 5


class TestReadOwner:
    @patch("dbt.adapters.scope.checkpoint.DataLakeServiceClient")
    def test_unclaimed_table_has_no_owner(self, mock_service):
        assert _owner_manager(mock_service, _FakeOwnerFile()).read_owner(_TABLE) is None

    @patch("dbt.adapters.scope.checkpoint.DataLakeServiceClient")
    def test_reads_the_owner(self, mock_service):
        fake = _FakeOwnerFile(_NEWER.to_json())
        assert _owner_manager(mock_service, fake).read_owner(_TABLE) == _NEWER

    @patch("dbt.adapters.scope.checkpoint.DataLakeServiceClient")
    def test_unreadable_owner_raises(self, mock_service):
        with pytest.raises(ValueError):
            _owner_manager(mock_service, _FakeOwnerFile("")).read_owner(_TABLE)
