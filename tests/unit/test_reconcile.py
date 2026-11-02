"""Unit tests for checkpoint recovery: pending batches, reconcile and legacy checkpoint migration."""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock

import pytest
from dbt_common.exceptions import DbtRuntimeError

from dbt.adapters.scope.adls_gen1_client import FileInfo
from dbt.adapters.scope.checkpoint import BATCH_TOKEN_PROPERTY, PendingBatch, Watermark
from dbt.adapters.scope.connections import ScopeConnectionHandle
from dbt.adapters.scope.delta_lake import DeltaCommit
from dbt.adapters.scope.file_tracker import FileTracker
from dbt.adapters.scope.impl import (
    ScopeAdapter,
    _adoptable_prefix,
    _match_source_files,
    _normalize_source_path,
)

LOC = "abfss://ctr@acct.dfs.core.windows.net/delta/events"
ROOT = "/shares/events/ss"
PATTERN = r".*\.ss$"
MODEL = "events"
SCOPE_ENGINE = "ScopeEngine/1.1.0 Delta-Lake/3.1.0"
SPARK = "Apache-Spark/3.5.1 Delta-Lake/3.2.0"
T0 = datetime(2026, 4, 1, tzinfo=timezone.utc)


def _schema(*columns: str) -> str:
    return json.dumps(
        {
            "type": "struct",
            "fields": [
                {
                    "name": name,
                    "type": "string",
                    "metadata": {"delta.columnMapping.physicalName": f"col-{name}"},
                }
                for name in columns
            ],
        }
    )


SCHEMA = _schema("id", "source_file_uri")


def _file(i: int, minute: int | None = None) -> FileInfo:
    path = f"{ROOT}/2026/04/01/file_{i:02d}.ss"
    return FileInfo(
        path=path,
        name=path.rsplit("/", 1)[-1],
        length=100,
        modification_time=T0 + timedelta(minutes=i if minute is None else minute),
    )


def _ms(dt: datetime) -> int:
    return int(dt.timestamp() * 1000)


def _uri(f: FileInfo) -> str:
    return f"adl://acct.azuredatalakestore.net{f.path}"


def _watermark_after(
    files: list[FileInfo], *, batch_id: int = 0, delta_version: int | None = None
) -> Watermark:
    return Watermark(
        version=batch_id,
        modified_time=max(f.modification_time for f in files).isoformat(),
        batch_id=batch_id,
        delta_version=delta_version,
    )


def _commit(
    version: int,
    *,
    token: str | None = None,
    engine: str = SCOPE_ENGINE,
    parquet: tuple[str, ...] = (),
    schema: str | None = SCHEMA,
) -> DeltaCommit:
    actions: list[dict] = [
        {
            "commitInfo": {
                "engineInfo": engine,
                "timestamp": _ms(T0 + timedelta(hours=version)),
            }
        }
    ]
    if schema is not None:
        configuration = {BATCH_TOKEN_PROPERTY: token} if token else {}
        actions.append({"metaData": {"schemaString": schema, "configuration": configuration}})
    actions += [{"add": {"path": path, "dataChange": True}} for path in parquet]
    return DeltaCommit(version=version, actions=actions)


def _pending(
    files: list[FileInfo],
    watermark_before: Watermark | None,
    delta_version_before: int,
    token: str = "tok-1",
) -> PendingBatch:
    return PendingBatch(
        token=token,
        batch_id=watermark_before.batch_id + 1 if watermark_before else 0,
        run_id="previous-run",
        job_name="events_incremental_batch_1_of_1_files_2",
        files=[(f.path, _ms(f.modification_time)) for f in files],
        watermark_before=watermark_before,
        delta_version_before=delta_version_before,
    )


class FakeCheckpoint:
    def __init__(
        self,
        watermark: Watermark | None = None,
        pending: PendingBatch | None = None,
        written_at: datetime | None = None,
    ) -> None:
        self.watermark = watermark
        self.pending = pending
        self.written_at = written_at
        self.batches: list[tuple[int, list[str]]] = []
        self.watermark_writes = 0
        self.reads = 0

    def read_watermark(self, delta_location: str, *, strict: bool = False) -> Watermark | None:
        self.reads += 1
        return None if self.watermark is None else Watermark.from_json(self.watermark.to_json())

    def write_watermark(self, delta_location: str, watermark: Watermark) -> None:
        self.watermark = Watermark.from_json(watermark.to_json())
        self.watermark_writes += 1

    def watermark_last_modified(self, delta_location: str) -> datetime | None:
        return self.written_at

    def read_pending(self, delta_location: str) -> PendingBatch | None:
        return self.pending

    def write_pending(self, delta_location: str, pending: PendingBatch) -> None:
        self.pending = pending

    def delete_pending(self, delta_location: str, *, strict: bool = False) -> None:
        self.pending = None

    def write_batch_sources(
        self, delta_location, batch_id, file_paths, modification_times, compaction_interval=10
    ) -> None:
        self.batches.append((batch_id, list(file_paths)))

    def cleanup_sources(self, delta_location: str, max_files: int = 0) -> None:
        pass


class FakeDeltaClient:
    def __init__(self, *commits: DeltaCommit) -> None:
        self.commits = {c.version: c for c in commits}
        self.checkpoint_version = -1
        self.checkpoint_metadata: dict | None = None
        self.uris: dict[str, set[str]] = {}
        self.columns_read: list[str] = []

    def add(self, commit: DeltaCommit) -> None:
        self.commits[commit.version] = commit

    def log_versions(self, delta_location: str) -> tuple[list[int], int]:
        versions = sorted(self.commits)
        return versions, max([-1, self.checkpoint_version, *versions])

    def latest_version(self, delta_location: str) -> int:
        return self.log_versions(delta_location)[1]

    def read_commit(self, delta_location: str, version: int) -> DeltaCommit | None:
        return self.commits.get(version)

    def read_commits(self, delta_location: str, versions) -> list[DeltaCommit]:
        return [self.commits[v] for v in sorted(versions) if v in self.commits]

    def latest_metadata_commit(self, delta_location: str) -> DeltaCommit | None:
        for version in sorted(self.commits, reverse=True):
            if version <= self.checkpoint_version:
                break
            if self.commits[version].metadata is not None:
                return self.commits[version]
        if self.checkpoint_metadata is None:
            return None
        return DeltaCommit(self.checkpoint_version, [{"metaData": self.checkpoint_metadata}])

    def distinct_column_values(self, delta_location: str, paths: list[str], column: str) -> set:
        self.columns_read.append(column)
        return set().union(*(self.uris.get(p, set()) for p in paths))


def _adapter(
    checkpoint: FakeCheckpoint, delta: FakeDeltaClient, files: list[FileInfo]
) -> ScopeAdapter:
    adapter = object.__new__(ScopeAdapter)
    gen1 = MagicMock()
    gen1.list_files.side_effect = lambda root, pattern=None: list(files)
    gen1.enrich_with_estimates.side_effect = lambda found: found
    adapter._gen1_client = gen1
    adapter._checkpoint_manager = checkpoint
    adapter._delta_client = delta
    adapter._file_tracker = FileTracker(gen1_client=gen1, checkpoint_manager=checkpoint)
    adapter.connections = MagicMock()
    return adapter


def _handle(adapter: ScopeAdapter) -> MagicMock:
    return adapter.connections.get_thread_connection.return_value.handle


def _reconcile(adapter: ScopeAdapter) -> None:
    adapter.reconcile_checkpoint(LOC, [ROOT], [PATTERN], MODEL)


def _unprocessed(adapter: ScopeAdapter, checkpoint: FakeCheckpoint) -> list[str]:
    return [f.path for f in adapter._list_unprocessed([ROOT], [PATTERN], checkpoint.watermark, 0)]


FILES = [_file(i) for i in range(6)]


class TestBeginBatch:
    def test_writes_pending_and_returns_the_batch_token(self):
        watermark = _watermark_after(FILES[:1], batch_id=3, delta_version=7)
        checkpoint = FakeCheckpoint(watermark)
        adapter = _adapter(checkpoint, FakeDeltaClient(_commit(7)), FILES)
        adapter._discovered_file_infos = {f.path: f for f in FILES[1:3]}

        properties = adapter.begin_batch(
            LOC, [f.path for f in FILES[1:3]], "events_incremental_batch_1_of_1_files_2"
        )

        pending = checkpoint.pending
        assert properties == {BATCH_TOKEN_PROPERTY: pending.token}
        assert pending.batch_id == 4
        assert pending.files == [(f.path, _ms(f.modification_time)) for f in FILES[1:3]]
        assert pending.watermark_before == watermark
        assert pending.delta_version_before == 7
        assert pending.run_id == ScopeConnectionHandle._run_id
        assert pending.job_name == "events_incremental_batch_1_of_1_files_2"

    def test_first_batch_of_a_new_table(self):
        checkpoint = FakeCheckpoint()
        adapter = _adapter(checkpoint, FakeDeltaClient(), FILES)
        adapter._discovered_file_infos = {FILES[0].path: FILES[0]}

        adapter.begin_batch(LOC, [FILES[0].path], "job")

        assert checkpoint.pending.batch_id == 0
        assert checkpoint.pending.watermark_before is None
        assert checkpoint.pending.delta_version_before == -1

    def test_every_batch_gets_a_new_token(self):
        checkpoint = FakeCheckpoint()
        adapter = _adapter(checkpoint, FakeDeltaClient(), FILES)
        adapter._discovered_file_infos = {FILES[0].path: FILES[0]}

        first = adapter.begin_batch(LOC, [FILES[0].path], "job")
        second = adapter.begin_batch(LOC, [FILES[0].path], "job")

        assert first[BATCH_TOKEN_PROPERTY] != second[BATCH_TOKEN_PROPERTY]

    def test_rejects_files_not_returned_by_discovery(self):
        checkpoint = FakeCheckpoint()
        adapter = _adapter(checkpoint, FakeDeltaClient(), FILES)
        adapter._discovered_file_infos = {}

        with pytest.raises(DbtRuntimeError, match="not returned by discover_files"):
            adapter.begin_batch(LOC, [FILES[0].path], "job")
        assert checkpoint.pending is None


class TestUpdateCheckpoint:
    def _begin(self, delta: FakeDeltaClient) -> tuple[ScopeAdapter, FakeCheckpoint, str]:
        checkpoint = FakeCheckpoint(_watermark_after(FILES[:1], delta_version=5))
        adapter = _adapter(checkpoint, delta, FILES)
        adapter._discovered_file_infos = {f.path: f for f in FILES}
        token = adapter.begin_batch(LOC, [f.path for f in FILES[1:3]], "job")[BATCH_TOKEN_PROPERTY]
        return adapter, checkpoint, token

    def _update(self, adapter: ScopeAdapter) -> None:
        adapter.update_checkpoint(LOC, [ROOT], [PATTERN], [f.path for f in FILES[1:3]])

    def test_records_the_version_carrying_the_batch_token(self):
        delta = FakeDeltaClient(_commit(5))
        adapter, checkpoint, token = self._begin(delta)
        delta.add(_commit(6, token=token, parquet=("part-6.parquet",)))
        delta.add(_commit(7, engine=SPARK, schema=None))

        self._update(adapter)

        assert checkpoint.watermark.delta_version == 6
        assert checkpoint.watermark.batch_id == 1
        assert checkpoint.watermark.modified_time == FILES[2].modification_time.isoformat()
        assert checkpoint.batches == [(1, [f.path for f in FILES[1:3]])]
        assert checkpoint.pending is None

    def test_falls_back_to_the_latest_version(self):
        delta = FakeDeltaClient(_commit(5))
        adapter, checkpoint, _ = self._begin(delta)
        delta.add(_commit(6, parquet=("part-6.parquet",)))

        self._update(adapter)

        assert checkpoint.watermark.delta_version == 6
        assert checkpoint.pending is None

    def test_unreadable_delta_log_still_checkpoints(self):
        delta = FakeDeltaClient(_commit(5))
        adapter, checkpoint, _ = self._begin(delta)
        delta.log_versions = MagicMock(side_effect=OSError("throttled"))

        self._update(adapter)

        assert checkpoint.watermark.batch_id == 1
        assert checkpoint.watermark.delta_version is None
        assert checkpoint.pending is None


class TestReconcilePending:
    def test_settles_orphaned_jobs_before_reading_the_checkpoint(self):
        checkpoint = FakeCheckpoint(_watermark_after(FILES[:1], delta_version=5))
        adapter = _adapter(checkpoint, FakeDeltaClient(_commit(5)), FILES)
        _handle(adapter).settle_orphaned_jobs.side_effect = DbtRuntimeError(
            "did not end within 600s"
        )

        with pytest.raises(DbtRuntimeError, match="did not end"):
            _reconcile(adapter)

        _handle(adapter).settle_orphaned_jobs.assert_called_once_with(MODEL)
        assert checkpoint.reads == 0

    def test_committed_pending_batch_is_checkpointed(self):
        watermark = _watermark_after(FILES[:1], delta_version=5)
        checkpoint = FakeCheckpoint(watermark, _pending(FILES[1:3], watermark, 5))
        delta = FakeDeltaClient(_commit(5), _commit(6, token="tok-1", parquet=("part-6.parquet",)))
        adapter = _adapter(checkpoint, delta, FILES)

        _reconcile(adapter)

        assert checkpoint.pending is None
        assert checkpoint.watermark.batch_id == 1
        assert checkpoint.watermark.delta_version == 6
        assert checkpoint.batches == [(1, [f.path for f in FILES[1:3]])]
        assert _unprocessed(adapter, checkpoint) == [f.path for f in FILES[3:]]
        assert delta.columns_read == []

    def test_uncommitted_pending_batch_is_discarded(self):
        watermark = _watermark_after(FILES[:1], delta_version=5)
        checkpoint = FakeCheckpoint(watermark, _pending(FILES[1:3], watermark, 5))
        adapter = _adapter(checkpoint, FakeDeltaClient(_commit(5)), FILES)

        _reconcile(adapter)

        assert checkpoint.pending is None
        assert checkpoint.watermark == watermark
        assert checkpoint.batches == []
        assert _unprocessed(adapter, checkpoint) == [f.path for f in FILES[1:]]

    def test_stale_pending_batch_is_discarded(self):
        before = _watermark_after(FILES[:1], delta_version=5)
        current = _watermark_after(FILES[:3], batch_id=1, delta_version=6)
        checkpoint = FakeCheckpoint(current, _pending(FILES[1:3], before, 5))
        delta = FakeDeltaClient(_commit(5), _commit(6, token="tok-1", parquet=("part-6.parquet",)))
        adapter = _adapter(checkpoint, delta, FILES)

        _reconcile(adapter)

        assert checkpoint.pending is None
        assert checkpoint.watermark == current
        assert checkpoint.batches == []

    def test_token_found_only_in_the_latest_checkpoint(self):
        watermark = _watermark_after(FILES[:1], delta_version=5)
        checkpoint = FakeCheckpoint(watermark, _pending(FILES[1:3], watermark, 5))
        delta = FakeDeltaClient(_commit(9, engine=SPARK, schema=None))
        delta.checkpoint_version = 8
        delta.checkpoint_metadata = {
            "schemaString": SCHEMA,
            "configuration": {BATCH_TOKEN_PROPERTY: "tok-1"},
        }
        adapter = _adapter(checkpoint, delta, FILES)

        _reconcile(adapter)

        assert checkpoint.pending is None
        assert checkpoint.watermark.batch_id == 1
        assert checkpoint.watermark.delta_version == 9
        assert checkpoint.batches == [(1, [f.path for f in FILES[1:3]])]

    def test_older_token_in_the_latest_checkpoint_is_not_a_match(self):
        watermark = _watermark_after(FILES[:1], delta_version=5)
        checkpoint = FakeCheckpoint(watermark, _pending(FILES[1:3], watermark, 5))
        delta = FakeDeltaClient(_commit(9, engine=SPARK, schema=None))
        delta.checkpoint_version = 8
        delta.checkpoint_metadata = {
            "schemaString": SCHEMA,
            "configuration": {BATCH_TOKEN_PROPERTY: "tok-0"},
        }
        adapter = _adapter(checkpoint, delta, FILES)

        _reconcile(adapter)

        assert checkpoint.pending is None
        assert checkpoint.watermark.same_position(watermark)
        assert checkpoint.watermark.delta_version == 9
        assert checkpoint.batches == []

    def test_first_batch_of_a_new_table_is_checkpointed(self):
        checkpoint = FakeCheckpoint(None, _pending(FILES[:2], None, -1))
        delta = FakeDeltaClient(_commit(0, token="tok-1", parquet=("part-0.parquet",)))
        adapter = _adapter(checkpoint, delta, FILES)

        _reconcile(adapter)

        assert checkpoint.pending is None
        assert checkpoint.watermark.batch_id == 0
        assert checkpoint.watermark.delta_version == 0
        assert checkpoint.batches == [(0, [f.path for f in FILES[:2]])]

    def test_new_table_without_a_commit_stays_unchecked(self):
        checkpoint = FakeCheckpoint(None, _pending(FILES[:2], None, -1))
        adapter = _adapter(checkpoint, FakeDeltaClient(), FILES)

        _reconcile(adapter)

        assert checkpoint.pending is None
        assert checkpoint.watermark is None
        assert checkpoint.watermark_writes == 0


class TestReconcileUnrecordedCommits:
    def _setup(self, *commits: DeltaCommit) -> tuple[ScopeAdapter, FakeCheckpoint, FakeDeltaClient]:
        checkpoint = FakeCheckpoint(_watermark_after(FILES[:1], delta_version=5))
        delta = FakeDeltaClient(_commit(5), *commits)
        return _adapter(checkpoint, delta, FILES), checkpoint, delta

    def test_adopts_files_ingested_by_commits_missing_from_the_checkpoint(self):
        adapter, checkpoint, delta = self._setup(
            _commit(6, parquet=("part-6a.parquet", "part-6b.parquet"))
        )
        delta.uris = {
            "part-6a.parquet": {_uri(FILES[1])},
            "part-6b.parquet": {_uri(FILES[2])},
        }

        _reconcile(adapter)

        assert delta.columns_read == ["col-source_file_uri"]
        assert checkpoint.batches == [(1, [f.path for f in FILES[1:3]])]
        assert checkpoint.watermark.modified_time == FILES[2].modification_time.isoformat()
        assert checkpoint.watermark.delta_version == 6
        assert _unprocessed(adapter, checkpoint) == [f.path for f in FILES[3:]]

    def test_adopts_only_the_contiguous_prefix(self):
        adapter, checkpoint, delta = self._setup(_commit(6, parquet=("part-6.parquet",)))
        delta.uris = {"part-6.parquet": {_uri(FILES[1]), _uri(FILES[3])}}

        _reconcile(adapter)

        assert checkpoint.batches == [(1, [FILES[1].path])]
        assert checkpoint.watermark.modified_time == FILES[1].modification_time.isoformat()
        assert _unprocessed(adapter, checkpoint) == [f.path for f in FILES[2:]]

    def test_file_sharing_a_modification_time_with_a_missed_file_is_not_adopted(self):
        files = [FILES[0], _file(1), _file(2, minute=1), FILES[3]]
        checkpoint = FakeCheckpoint(_watermark_after(files[:1], delta_version=5))
        delta = FakeDeltaClient(_commit(5), _commit(6, parquet=("part-6.parquet",)))
        delta.uris = {"part-6.parquet": {_uri(files[1])}}
        adapter = _adapter(checkpoint, delta, files)

        _reconcile(adapter)

        assert checkpoint.batches == []
        assert checkpoint.watermark.batch_id == 0
        assert checkpoint.watermark.delta_version == 6
        assert _unprocessed(adapter, checkpoint) == [f.path for f in files[1:]]

    def test_ignores_commits_not_written_by_scope_engine(self):
        adapter, checkpoint, delta = self._setup(
            _commit(6, engine=SPARK, parquet=("part-6.parquet",))
        )
        delta.uris = {"part-6.parquet": {_uri(FILES[1])}}

        _reconcile(adapter)

        assert delta.columns_read == []
        assert checkpoint.batches == []
        assert checkpoint.watermark.delta_version == 6

    def test_table_without_source_file_column_only_moves_the_baseline(self):
        adapter, checkpoint, delta = self._setup(
            _commit(6, parquet=("part-6.parquet",), schema=_schema("id"))
        )

        _reconcile(adapter)

        assert delta.columns_read == []
        assert checkpoint.batches == []
        assert checkpoint.watermark.batch_id == 0
        assert checkpoint.watermark.delta_version == 6

    def test_schema_read_from_table_metadata_when_commits_carry_none(self):
        adapter, checkpoint, delta = self._setup(
            _commit(6, parquet=("part-6.parquet",), schema=None)
        )
        delta.uris = {"part-6.parquet": {_uri(FILES[1])}}

        _reconcile(adapter)

        assert delta.columns_read == ["col-source_file_uri"]
        assert checkpoint.batches == [(1, [FILES[1].path])]

    def test_read_failure_warns_and_moves_the_baseline(self):
        adapter, checkpoint, delta = self._setup(_commit(6, parquet=("part-6.parquet",)))
        delta.distinct_column_values = MagicMock(side_effect=OSError("parquet unreadable"))

        _reconcile(adapter)

        assert checkpoint.batches == []
        assert checkpoint.watermark.batch_id == 0
        assert checkpoint.watermark.delta_version == 6

    def test_up_to_date_checkpoint_is_not_rewritten(self):
        adapter, checkpoint, _ = self._setup()

        _reconcile(adapter)

        assert checkpoint.watermark_writes == 0


class TestLegacyCheckpointMigration:
    def test_adopts_commits_newer_than_the_watermark_file(self):
        checkpoint = FakeCheckpoint(
            _watermark_after(FILES[:1]), written_at=T0 + timedelta(hours=4, minutes=30)
        )
        delta = FakeDeltaClient(
            _commit(3, parquet=("part-3.parquet",)),
            _commit(4, parquet=("part-4.parquet",)),
            _commit(5, parquet=("part-5.parquet",)),
            _commit(6, engine=SPARK, schema=None, parquet=("compacted.parquet",)),
        )
        delta.uris = {
            "part-4.parquet": {_uri(FILES[0])},
            "part-5.parquet": {_uri(FILES[1]), _uri(FILES[2])},
            "compacted.parquet": {_uri(FILES[3])},
        }
        adapter = _adapter(checkpoint, delta, FILES)

        _reconcile(adapter)

        assert checkpoint.batches == [(1, [f.path for f in FILES[1:3]])]
        assert checkpoint.watermark.delta_version == 6
        assert _unprocessed(adapter, checkpoint) == [f.path for f in FILES[3:]]

    def test_records_the_latest_version_when_nothing_is_missing(self):
        watermark = _watermark_after(FILES[:1])
        checkpoint = FakeCheckpoint(watermark, written_at=T0 + timedelta(hours=10))
        delta = FakeDeltaClient(_commit(3), _commit(4, parquet=("part-4.parquet",)))
        adapter = _adapter(checkpoint, delta, FILES)

        _reconcile(adapter)

        assert checkpoint.batches == []
        assert checkpoint.watermark.same_position(watermark)
        assert checkpoint.watermark.delta_version == 4
        assert delta.columns_read == []

    def test_no_checkpoint_has_nothing_to_reconcile(self):
        checkpoint = FakeCheckpoint()
        delta = FakeDeltaClient(_commit(0, parquet=("part-0.parquet",)))
        adapter = _adapter(checkpoint, delta, FILES)

        _reconcile(adapter)

        _handle(adapter).settle_orphaned_jobs.assert_called_once_with(MODEL)
        assert checkpoint.watermark is None
        assert delta.columns_read == []


class TestSourcePathMatching:
    def test_normalizes_scheme_host_encoding_and_slashes(self):
        assert _normalize_source_path("adl://a.azuredatalakestore.net/x/a%20b.ss") == "/x/a b.ss"
        assert _normalize_source_path("shares//x/y.ss?ver=1") == "/shares/x/y.ss"
        assert _normalize_source_path(" /shares/x/y.ss ") == "/shares/x/y.ss"

    def test_matches_on_path_suffix(self):
        files = [_file(i) for i in range(1, 6)]
        uris = {
            f"adl://acct.azuredatalakestore.net{files[0].path}",
            f"https://acct.dfs.core.windows.net/fs{files[1].path.replace('_', '%5F')}",
            f"/mnt/local{files[2].path}",
            f"{ROOT}/2026/04/01/xfile_04.ss",
            f"/other/2026/04/01/{files[4].name}",
        }

        assert _match_source_files(uris, files) == {f.path for f in files[:3]}

    def test_adoptable_prefix(self):
        files = [_file(1), _file(2), _file(3)]
        assert _adoptable_prefix(files, {f.path for f in files}) == files
        assert _adoptable_prefix(files, {files[0].path, files[2].path}) == files[:1]
        assert _adoptable_prefix(files, {files[1].path}) == []
        assert _adoptable_prefix(files, set()) == []
