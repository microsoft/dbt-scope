"""Checkpoint manager — watermark + sources persistence for file-based processing.

Reads and writes ``_checkpoint/watermark.json`` and per-batch JSONL files in
``_checkpoint/sources/`` alongside ``_delta_log/`` in the Delta table root on
ADLS Gen2.

Watermark schema::

    {
        "version": 0,
        "modifiedTime": "2026-04-01T12:34:56.789000+00:00",
        "batchId": 3,
        "deltaVersion": 42
    }

``deltaVersion`` is the Delta table version the checkpoint covers; it is
absent in checkpoints written by older adapter versions.

Sources JSONL (one line per processed file)::

    {"path": "/local/.../file.ss", "modificationTime": 1775018672000, "batchId": 0, "batchProcessingTime": "2026-04-06T21:00:00+00:00"}

``_checkpoint/pending.json`` records the batch that was submitted but not yet
checkpointed (see :class:`PendingBatch`). Each batch stamps its token into the
Delta commit as the ``dbt_scope.batch_token`` table property, which lets the
next run tell whether that batch committed.

``_checkpoint/owner.json`` names the dbt run that owns the table (see
:class:`TableOwner`)::

    {"runId": "6f1c...", "runStartedAt": "2026-11-02T00:26:58.123456+00:00"}
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from azure.core import MatchConditions
from azure.core.credentials import TokenCredential
from azure.core.exceptions import (
    ResourceExistsError,
    ResourceModifiedError,
    ResourceNotFoundError,
)
from azure.identity import CredentialUnavailableError
from azure.storage.filedatalake import DataLakeServiceClient
from dbt.adapters.events.logging import AdapterLogger

from dbt.adapters.scope.delta_lake import AbfssLocation, RetryPolicy
from dbt.adapters.scope.message_retry import MessageRetryPolicy, retry_on_message

log = AdapterLogger("scope")

_CHECKPOINT_DIR = "_checkpoint"
_WATERMARK_FILE = "watermark.json"
_PENDING_FILE = "pending.json"
_OWNER_FILE = "owner.json"
_SOURCES_DIR = "sources"
_OWNER_CLAIM_ATTEMPTS = 5

BATCH_TOKEN_PROPERTY = "dbt_scope.batch_token"
PENDING_SCHEMA_VERSION = 1


def _json_default(o: object) -> str:
    """``json.dumps`` ``default=`` hook for source records.

    Source records can carry ``datetime`` values when they originate from
    a previously written parquet snapshot (DuckDB returns ``TIMESTAMP``
    columns as Python ``datetime``). Convert them back to ISO 8601 strings
    so the records round-trip through NDJSON cleanly.

    DuckDB's ``TIMESTAMP`` is naive — it drops the timezone offset on the
    cast that produces the snapshot — but every value produced inside
    this module is created from ``datetime.now(timezone.utc)``. So if
    the datetime comes back naive we re-attach UTC, preserving the
    timezone-aware ISO 8601 contract that JSONL diffs use.
    """
    if isinstance(o, datetime):
        if o.tzinfo is None:
            o = o.replace(tzinfo=timezone.utc)
        return o.isoformat()
    raise TypeError(f"Object of type {type(o).__name__} is not JSON serializable")


# Virtual column names that map to SCOPE FILE.* functions
VIRTUAL_COLUMNS: dict[str, str] = {
    "source_file_uri": "FILE.URI()",
    "source_file_length": "FILE.LENGTH()",
    "source_file_created": "FILE.CREATED()",
    "source_file_modified": "FILE.MODIFIED()",
}


@dataclass
class Watermark:
    """Persisted watermark state for file-based processing."""

    version: int = 0
    modified_time: str = ""  # ISO-8601, e.g. "2026-04-01T12:34:56.789000+00:00"
    batch_id: int = 0  # Resets on full refresh
    delta_version: int | None = None

    @property
    def modified_time_dt(self) -> datetime | None:
        """Parse ``modified_time`` to a timezone-aware ``datetime``, or None."""
        if not self.modified_time:
            return None
        return datetime.fromisoformat(self.modified_time)

    def same_position(self, other: Watermark | None) -> bool:
        """Return ``True`` if *other* tracks the same processed-file position."""
        return other is not None and (self.version, self.modified_time, self.batch_id) == (
            other.version,
            other.modified_time,
            other.batch_id,
        )

    def to_dict(self) -> dict[str, Any]:
        data: dict[str, Any] = {
            "version": self.version,
            "modifiedTime": self.modified_time,
            "batchId": self.batch_id,
        }
        if self.delta_version is not None:
            data["deltaVersion"] = self.delta_version
        return data

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), indent=2)

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> Watermark:
        return cls(
            version=data.get("version", 0),
            modified_time=data.get("modifiedTime", ""),
            batch_id=data.get("batchId", 0),
            delta_version=data.get("deltaVersion"),
        )

    @classmethod
    def from_json(cls, raw: str) -> Watermark:
        return cls.from_dict(json.loads(raw))


@dataclass
class PendingBatch:
    """A submitted batch whose checkpoint has not been written yet.

    ``watermark_before`` is the watermark the batch was discovered from; the
    record is stale once the watermark moves past it. ``delta_version_before``
    is the latest Delta version before submission (``-1`` if no table yet).
    """

    token: str
    batch_id: int
    run_id: str
    job_name: str
    files: list[tuple[str, int]]  # (path, modificationTime in epoch ms)
    watermark_before: Watermark | None
    delta_version_before: int
    created_at: str = field(default_factory=lambda: datetime.now(timezone.utc).isoformat())
    schema_version: int = PENDING_SCHEMA_VERSION

    def applies_to(self, watermark: Watermark | None) -> bool:
        if self.watermark_before is None:
            return watermark is None
        return self.watermark_before.same_position(watermark)

    @property
    def file_paths(self) -> list[str]:
        return [path for path, _ in self.files]

    @property
    def modification_times(self) -> list[datetime]:
        return [datetime.fromtimestamp(ms / 1000, tz=timezone.utc) for _, ms in self.files]

    def to_json(self) -> str:
        return json.dumps(
            {
                "schemaVersion": self.schema_version,
                "batchId": self.batch_id,
                "token": self.token,
                "runId": self.run_id,
                "jobName": self.job_name,
                "files": [{"path": p, "modificationTime": ms} for p, ms in self.files],
                "watermarkBefore": (
                    self.watermark_before.to_dict() if self.watermark_before else None
                ),
                "deltaVersionBefore": self.delta_version_before,
                "createdAt": self.created_at,
            },
            indent=2,
        )

    @classmethod
    def from_json(cls, raw: str) -> PendingBatch:
        data = json.loads(raw)
        watermark_before = data.get("watermarkBefore")
        return cls(
            token=data["token"],
            batch_id=data.get("batchId", 0),
            run_id=data.get("runId", ""),
            job_name=data.get("jobName", ""),
            files=[(f["path"], int(f["modificationTime"])) for f in data.get("files", [])],
            watermark_before=Watermark.from_dict(watermark_before) if watermark_before else None,
            delta_version_before=int(data.get("deltaVersionBefore", -1)),
            created_at=data.get("createdAt", ""),
            schema_version=data.get("schemaVersion", PENDING_SCHEMA_VERSION),
        )


@dataclass(frozen=True)
class TableOwner:
    """The dbt run allowed to submit batch jobs for a table and write its checkpoint.

    Runs are ranked by start time, then run ID: a run that started later takes a
    table over from an earlier one, never the reverse.
    """

    run_id: str
    run_started_at: datetime

    def outranks(self, other: TableOwner) -> bool:
        return (self.run_started_at, self.run_id) > (other.run_started_at, other.run_id)

    def to_json(self) -> str:
        return json.dumps(
            {"runId": self.run_id, "runStartedAt": self.run_started_at.isoformat()}, indent=2
        )

    @classmethod
    def from_json(cls, raw: str) -> TableOwner:
        """Parse ``owner.json``; raises ``ValueError`` when it is not a valid record."""
        try:
            data = json.loads(raw)
            owner = cls(
                run_id=str(data["runId"]),
                run_started_at=datetime.fromisoformat(data["runStartedAt"]),
            )
        except (KeyError, TypeError) as exc:
            raise ValueError(f"invalid {_OWNER_FILE}: {exc!r}") from exc
        if owner.run_started_at.tzinfo is None:
            raise ValueError(f"invalid {_OWNER_FILE}: runStartedAt has no timezone")
        return owner


def _download_text(file_client: Any) -> tuple[str | None, str | None]:
    """Return ``(content, etag)`` of a file, or ``(None, None)`` if it does not exist."""
    try:
        download = file_client.download_file()
    except ResourceNotFoundError:
        return None, None
    return download.readall().decode("utf-8"), download.properties.etag


def _get_service(parsed: AbfssLocation, credential: TokenCredential) -> DataLakeServiceClient:
    return DataLakeServiceClient(account_url=parsed.account_url, credential=credential)


def _checkpoint_dir(parsed: AbfssLocation) -> str:
    return f"{parsed.path.rstrip('/')}/{_CHECKPOINT_DIR}"


def _checkpoint_file(parsed: AbfssLocation, name: str) -> str:
    return f"{_checkpoint_dir(parsed)}/{name}"


class CheckpointManager:
    """Manage ``_checkpoint/`` on ADLS Gen2 Delta table roots."""

    def __init__(
        self,
        *,
        credential: TokenCredential | None = None,
        retry_policy: RetryPolicy | None = None,
        message_retry_policy: MessageRetryPolicy | None = None,
    ) -> None:
        if credential is None:
            raise RuntimeError(
                "CheckpointManager requires an explicit ``credential``; "
                "callers should pass ``credential=build_credential(creds)``."
            )
        self._credential = credential
        self._retry_policy = retry_policy
        self._message_retry_policy = message_retry_policy or MessageRetryPolicy.disabled()

    def _retry(self, op, *, label: str):
        return retry_on_message(op, policy=self._message_retry_policy, label=label)

    # -- Watermark ---------------------------------------------------------

    def read_watermark(self, delta_location: str, *, strict: bool = False) -> Watermark | None:
        """Read the watermark from ``_checkpoint/watermark.json``.

        Returns ``None`` when no checkpoint exists. Other read errors also return
        ``None`` unless *strict* is set, in which case they propagate.
        """
        parsed = AbfssLocation.parse(delta_location)
        if parsed is None:
            log.warning(f"read_watermark: invalid delta_location: {delta_location}")
            return None

        def _read() -> Watermark | None:
            service = _get_service(parsed, self._credential)
            fs = service.get_file_system_client(parsed.container)
            file_path = f"{parsed.path.rstrip('/')}/{_CHECKPOINT_DIR}/{_WATERMARK_FILE}"
            file_client = fs.get_file_client(file_path)
            try:
                download = file_client.download_file()
            except ResourceNotFoundError:
                return None
            raw = download.readall().decode("utf-8")
            watermark = Watermark.from_json(raw)
            log.debug(
                f"Read watermark: version={watermark.version}, "
                f"modified_time={watermark.modified_time}, "
                f"batch_id={watermark.batch_id}, "
                f"delta_version={watermark.delta_version}"
            )
            return watermark

        try:
            watermark = self._retry(_read, label=f"checkpoint.read_watermark {delta_location}")
        except CredentialUnavailableError:
            # Don't mask auth failures as "no checkpoint" — that would
            # silently flip an incremental run into a full refresh.
            log.error(f"read_watermark: credential acquisition exhausted for {delta_location}")
            raise
        except Exception:
            if strict:
                raise
            log.debug(f"No checkpoint found for {delta_location} (first run or full refresh)")
            return None
        if watermark is None:
            log.debug(f"No checkpoint found for {delta_location} (first run or full refresh)")
        return watermark

    def write_watermark(self, delta_location: str, watermark: Watermark) -> None:
        """Write (create or overwrite) the watermark checkpoint."""
        parsed = AbfssLocation.parse(delta_location)
        if parsed is None:
            log.warning(f"write_watermark: invalid delta_location: {delta_location}")
            return

        def _write() -> None:
            service = _get_service(parsed, self._credential)
            fs = service.get_file_system_client(parsed.container)

            dir_path = f"{parsed.path.rstrip('/')}/{_CHECKPOINT_DIR}"
            dir_client = fs.get_directory_client(dir_path)
            dir_client.create_directory()

            file_path = f"{dir_path}/{_WATERMARK_FILE}"
            file_client = fs.get_file_client(file_path)
            data = watermark.to_json().encode("utf-8")
            file_client.upload_data(data, overwrite=True)

            log.debug(
                f"Wrote watermark: version={watermark.version}, "
                f"modified_time={watermark.modified_time}, "
                f"batch_id={watermark.batch_id}, "
                f"delta_version={watermark.delta_version} → {delta_location}"
            )

        try:
            self._retry(_write, label=f"checkpoint.write_watermark {delta_location}")
        except Exception:
            log.error(f"write_watermark failed for {delta_location}")
            raise

    def delete_watermark(self, delta_location: str) -> None:
        """Delete the watermark checkpoint and all sources (for full refresh)."""
        parsed = AbfssLocation.parse(delta_location)
        if parsed is None:
            log.warning(f"delete_watermark: invalid delta_location: {delta_location}")
            return

        def _delete() -> None:
            service = _get_service(parsed, self._credential)
            fs = service.get_file_system_client(parsed.container)
            file_path = f"{parsed.path.rstrip('/')}/{_CHECKPOINT_DIR}/{_WATERMARK_FILE}"
            file_client = fs.get_file_client(file_path)
            file_client.delete_file()
            log.debug(f"Deleted watermark for {delta_location}")

        try:
            self._retry(_delete, label=f"checkpoint.delete_watermark {delta_location}")
        except Exception:
            log.debug(f"No watermark to delete for {delta_location} (already clean)")

        self.delete_pending(delta_location, strict=True)
        # Also delete all sources
        self.delete_all_sources(delta_location)

    def watermark_last_modified(self, delta_location: str) -> datetime | None:
        """Return when ``watermark.json`` was last written, or ``None`` if it does not exist."""
        parsed = AbfssLocation.parse(delta_location)
        if parsed is None:
            return None

        def _properties() -> datetime | None:
            service = _get_service(parsed, self._credential)
            fs = service.get_file_system_client(parsed.container)
            file_client = fs.get_file_client(_checkpoint_file(parsed, _WATERMARK_FILE))
            try:
                return file_client.get_file_properties().last_modified
            except ResourceNotFoundError:
                return None

        return self._retry(
            _properties, label=f"checkpoint.watermark_last_modified {delta_location}"
        )

    # -- Pending batch -----------------------------------------------------

    def read_pending(self, delta_location: str) -> PendingBatch | None:
        """Read ``_checkpoint/pending.json``; ``None`` if absent or unreadable."""
        parsed = AbfssLocation.parse(delta_location)
        if parsed is None:
            return None

        def _read() -> str | None:
            service = _get_service(parsed, self._credential)
            fs = service.get_file_system_client(parsed.container)
            file_client = fs.get_file_client(_checkpoint_file(parsed, _PENDING_FILE))
            try:
                return file_client.download_file().readall().decode("utf-8")
            except ResourceNotFoundError:
                return None

        raw = self._retry(_read, label=f"checkpoint.read_pending {delta_location}")
        if raw is None:
            return None
        try:
            return PendingBatch.from_json(raw)
        except (ValueError, KeyError, TypeError) as exc:
            log.warning(f"Ignoring unreadable {_PENDING_FILE} for {delta_location}: {exc}")
            return None

    def write_pending(self, delta_location: str, pending: PendingBatch) -> None:
        """Write (create or overwrite) ``_checkpoint/pending.json``."""
        parsed = AbfssLocation.parse(delta_location)
        if parsed is None:
            raise ValueError(f"write_pending: invalid delta_location: {delta_location}")

        def _write() -> None:
            service = _get_service(parsed, self._credential)
            fs = service.get_file_system_client(parsed.container)
            fs.get_directory_client(_checkpoint_dir(parsed)).create_directory()
            file_client = fs.get_file_client(_checkpoint_file(parsed, _PENDING_FILE))
            file_client.upload_data(pending.to_json().encode("utf-8"), overwrite=True)
            log.debug(
                f"Wrote pending batch {pending.batch_id} (token={pending.token}, "
                f"{len(pending.files)} files) → {delta_location}"
            )

        self._retry(_write, label=f"checkpoint.write_pending {delta_location}")

    def delete_pending(self, delta_location: str, *, strict: bool = False) -> None:
        """Delete ``_checkpoint/pending.json`` if present; errors only warn unless *strict*."""
        parsed = AbfssLocation.parse(delta_location)
        if parsed is None:
            return

        def _delete() -> None:
            service = _get_service(parsed, self._credential)
            fs = service.get_file_system_client(parsed.container)
            file_client = fs.get_file_client(_checkpoint_file(parsed, _PENDING_FILE))
            try:
                file_client.delete_file()
            except ResourceNotFoundError:
                return

        try:
            self._retry(_delete, label=f"checkpoint.delete_pending {delta_location}")
        except CredentialUnavailableError:
            raise
        except Exception as exc:
            if strict:
                raise
            log.warning(f"delete_pending failed for {delta_location}: {exc}")

    # -- Table owner -------------------------------------------------------

    def read_owner(self, delta_location: str) -> TableOwner | None:
        """Read ``_checkpoint/owner.json``; ``None`` if no run has claimed the table.

        Raises ``ValueError`` when the file is not a valid record, which is also what a
        reader sees while another run is rewriting it.
        """
        parsed = AbfssLocation.parse(delta_location)
        if parsed is None:
            raise ValueError(f"read_owner: invalid delta_location: {delta_location}")

        def _read() -> str | None:
            service = _get_service(parsed, self._credential)
            fs = service.get_file_system_client(parsed.container)
            return _download_text(fs.get_file_client(_checkpoint_file(parsed, _OWNER_FILE)))[0]

        raw = self._retry(_read, label=f"checkpoint.read_owner {delta_location}")
        return None if raw is None else TableOwner.from_json(raw)

    def claim_owner(self, delta_location: str, claimant: TableOwner) -> TableOwner:
        """Make *claimant* the table's owner unless a run that outranks it already is.

        Returns the owner after the call: *claimant*, or the run that keeps the table.
        The write is conditional on the ETag that was read (or on the file being absent),
        so two runs claiming at once cannot both win.
        """
        parsed = AbfssLocation.parse(delta_location)
        if parsed is None:
            raise ValueError(f"claim_owner: invalid delta_location: {delta_location}")
        data = claimant.to_json().encode("utf-8")

        def _claim() -> TableOwner:
            service = _get_service(parsed, self._credential)
            fs = service.get_file_system_client(parsed.container)
            file_client = fs.get_file_client(_checkpoint_file(parsed, _OWNER_FILE))
            for _ in range(_OWNER_CLAIM_ATTEMPTS):
                raw, etag = _download_text(file_client)
                current: TableOwner | None = None
                if raw is not None:
                    try:
                        current = TableOwner.from_json(raw)
                    except ValueError as exc:
                        log.debug(f"Replacing unreadable {_OWNER_FILE} of {delta_location}: {exc}")
                if current is not None and (current == claimant or current.outranks(claimant)):
                    return current
                try:
                    if etag is None:
                        fs.get_directory_client(_checkpoint_dir(parsed)).create_directory()
                        file_client.upload_data(
                            data, overwrite=True, match_condition=MatchConditions.IfMissing
                        )
                    else:
                        file_client.upload_data(
                            data,
                            overwrite=True,
                            etag=etag,
                            match_condition=MatchConditions.IfNotModified,
                        )
                except (ResourceExistsError, ResourceModifiedError):
                    continue
                return claimant
            raise RuntimeError(
                f"Could not claim {delta_location}: {_OWNER_FILE} changed during each of "
                f"{_OWNER_CLAIM_ATTEMPTS} attempts"
            )

        return self._retry(_claim, label=f"checkpoint.claim_owner {delta_location}")

    # -- Sources -----------------------------------------------------------

    def write_batch_sources(
        self,
        delta_location: str,
        batch_id: int,
        file_paths: list[str],
        modification_times: list[datetime],
        compaction_interval: int = 10,
    ) -> None:
        """Record processed files for a batch.

        On compaction boundaries (``batch_id > 0`` and
        ``batch_id % compaction_interval == 0``), a **parquet snapshot** is
        written containing ALL history (previous snapshot + JSONL diffs since
        that snapshot + this batch).  Otherwise a JSONL diff is written.

        JSONL diffs and parquet snapshots are **never** deleted by compaction —
        they remain on disk.  File count is bounded separately by
        :meth:`cleanup_sources` (retention).

        Layout after 21 batches with ``compaction_interval=10``::

            _checkpoint/sources/
              0             ← JSONL diff (batch 0)
              1             ← JSONL diff
              ...
              9             ← JSONL diff
              10.parquet    ← full snapshot (batches 0-10)
              11            ← JSONL diff
              ...
              19            ← JSONL diff
              20.parquet    ← full snapshot (batches 0-20)
        """
        parsed = AbfssLocation.parse(delta_location)
        if parsed is None:
            return

        now = datetime.now(timezone.utc).isoformat()
        batch_records = self._build_source_records(file_paths, modification_times, batch_id, now)

        is_compaction = batch_id > 0 and batch_id % compaction_interval == 0

        def _write() -> None:
            service = _get_service(parsed, self._credential)
            fs = service.get_file_system_client(parsed.container)
            sources_dir = f"{parsed.path.rstrip('/')}/{_CHECKPOINT_DIR}/{_SOURCES_DIR}"
            dir_client = fs.get_directory_client(sources_dir)
            dir_client.create_directory()

            if is_compaction:
                self._write_snapshot_parquet(fs, sources_dir, batch_id, batch_records)
            else:
                self._write_jsonl(fs, sources_dir, batch_id, batch_records)

            log.debug(
                f"Wrote sources batch {batch_id} ({len(file_paths)} files, "
                f"{'parquet snapshot' if is_compaction else 'jsonl diff'}) → "
                f"{delta_location}"
            )

        try:
            self._retry(_write, label=f"checkpoint.write_batch_sources batch={batch_id}")
        except Exception:
            log.error(f"write_batch_sources failed for batch {batch_id}")
            raise

    @staticmethod
    def _build_source_records(
        file_paths: list[str],
        modification_times: list[datetime],
        batch_id: int,
        processing_time: str,
    ) -> list[dict]:
        result: list[dict] = []
        for path, mod_time in zip(file_paths, modification_times, strict=True):
            result.append(
                {
                    "path": path,
                    "modificationTime": int(mod_time.timestamp() * 1000),
                    "batchId": batch_id,
                    "batchProcessingTime": processing_time,
                }
            )
        return result

    @staticmethod
    def _write_jsonl(fs, sources_dir: str, batch_id: int, records: list[dict]) -> None:
        lines = [json.dumps(r, default=_json_default, separators=(",", ":")) for r in records]
        content = "\n".join(lines)
        file_path = f"{sources_dir}/{batch_id}"
        file_client = fs.get_file_client(file_path)
        file_client.upload_data(content.encode("utf-8"), overwrite=True)

    def _write_snapshot_parquet(
        self, fs, sources_dir: str, batch_id: int, current_batch_records: list[dict]
    ) -> None:
        """Write a full-history parquet snapshot (Spark-style compaction).

        Reads the most recent parquet snapshot (if any) + JSONL diffs written
        since that snapshot + the current batch records, and writes a single
        ``{batch_id}.parquet``.  Old parquet snapshots and JSONL diffs are
        **never** deleted here — that is handled by :meth:`cleanup_sources`.
        """
        import os

        import duckdb

        # Collect all history: latest snapshot + JSONL diffs since
        all_records: list[dict] = []
        snapshot_batch_id: int = -1
        latest_snapshot_path: str | None = None

        # Two-pass: first find the latest parquet snapshot, then read JSONL diffs
        # that arrived AFTER the snapshot (to avoid double-counting).
        file_entries: list[tuple[str, str]] = []  # (name, full_path)
        for path_info in fs.get_paths(path=sources_dir, recursive=False):
            if getattr(path_info, "is_directory", False):
                continue
            name = path_info.name.rsplit("/", 1)[-1]
            file_entries.append((name, path_info.name))

            if name.endswith(".parquet"):
                try:
                    snap_id = int(name.removesuffix(".parquet"))
                    if snap_id > snapshot_batch_id:
                        snapshot_batch_id = snap_id
                        latest_snapshot_path = path_info.name
                except ValueError:
                    pass

        # Read the latest parquet snapshot (skip older ones — they're subsets)
        if latest_snapshot_path is not None:
            try:
                file_client = fs.get_file_client(latest_snapshot_path)
                parquet_bytes = file_client.download_file().readall()
                snap_name = latest_snapshot_path.rsplit("/", 1)[-1]
                tmp_path = f"/tmp/dbt_scope_read_{snap_name}"
                with open(tmp_path, "wb") as tmp_f:
                    tmp_f.write(parquet_bytes)
                conn = duckdb.connect()
                try:
                    rows = conn.execute(f"SELECT * FROM read_parquet('{tmp_path}')").fetchall()
                    cols = [
                        d[0]
                        for d in conn.execute(
                            f"DESCRIBE SELECT * FROM read_parquet('{tmp_path}')"
                        ).fetchall()
                    ]
                    for row in rows:
                        all_records.append(dict(zip(cols, row, strict=False)))
                finally:
                    conn.close()
                    os.remove(tmp_path)
            except Exception:
                log.warning(f"Failed to read snapshot {latest_snapshot_path}")

        # Read JSONL diffs written after the latest snapshot
        for name, full_path in file_entries:
            if name.endswith(".parquet"):
                continue

            try:
                jsonl_batch_id = int(name)
            except ValueError:
                continue

            if jsonl_batch_id <= snapshot_batch_id:
                continue

            try:
                file_client = fs.get_file_client(full_path)
                raw = file_client.download_file().readall().decode("utf-8")
                for line in raw.strip().split("\n"):
                    if line.strip():
                        all_records.append(json.loads(line))
            except Exception:
                log.warning(f"Failed to read JSONL {name}")

        # Add current batch records
        all_records.extend(current_batch_records)

        # Write consolidated parquet via DuckDB (NDJSON → read_json_auto → COPY).
        #
        # ``batchProcessingTime`` is written here as an ISO 8601 string, but
        # DuckDB's ``read_json_auto`` will infer it as ``TIMESTAMP`` once the
        # NDJSON has enough rows of consistent ISO text. We force the cast
        # explicitly so the parquet schema is deterministic regardless of
        # sample size — and so subsequent reads of this snapshot always come
        # back as ``datetime`` (handled by ``_json_default`` on the next
        # round-trip).
        parquet_local = f"/tmp/dbt_scope_{batch_id}.parquet"
        ndjson_local = f"/tmp/dbt_scope_{batch_id}.ndjson"
        try:
            with open(ndjson_local, "w") as nf:
                for r in all_records:
                    nf.write(json.dumps(r, default=_json_default) + "\n")

            conn = duckdb.connect()
            try:
                # Cast every column explicitly so the snapshot schema is
                # fully deterministic regardless of DuckDB's
                # ``read_json_auto`` heuristics (which vary with sample
                # size and version).
                conn.execute(
                    "CREATE TABLE sources AS "
                    "SELECT "
                    "CAST(path AS VARCHAR) AS path, "
                    'CAST("modificationTime" AS BIGINT) AS "modificationTime", '
                    'CAST("batchId" AS BIGINT) AS "batchId", '
                    'CAST("batchProcessingTime" AS TIMESTAMP) AS "batchProcessingTime" '
                    f"FROM read_json_auto('{ndjson_local}')"
                )
                conn.execute(f"COPY sources TO '{parquet_local}' (FORMAT PARQUET)")
            finally:
                conn.close()
        finally:
            if os.path.exists(ndjson_local):
                os.remove(ndjson_local)

        with open(parquet_local, "rb") as f:
            parquet_data = f.read()

        dest_path = f"{sources_dir}/{batch_id}.parquet"
        file_client = fs.get_file_client(dest_path)
        file_client.upload_data(parquet_data, overwrite=True)
        os.remove(parquet_local)

        log.debug(f"Wrote snapshot {batch_id}.parquet ({len(all_records)} total records)")

    def cleanup_sources(
        self,
        delta_location: str,
        max_files: int = 100,
    ) -> int:
        """Delete oldest files in ``_checkpoint/sources/`` if count exceeds *max_files*.

        Returns the number of files deleted.
        """
        parsed = AbfssLocation.parse(delta_location)
        if parsed is None:
            return 0

        def _cleanup() -> int:
            service = _get_service(parsed, self._credential)
            fs = service.get_file_system_client(parsed.container)
            sources_dir = f"{parsed.path.rstrip('/')}/{_CHECKPOINT_DIR}/{_SOURCES_DIR}"

            files: list[tuple[str, str]] = []  # (name, full_path)
            for path_info in fs.get_paths(path=sources_dir, recursive=False):
                if getattr(path_info, "is_directory", False):
                    continue
                name = path_info.name.rsplit("/", 1)[-1]
                files.append((name, path_info.name))

            if len(files) <= max_files:
                return 0

            def sort_key(item: tuple[str, str]) -> tuple[int, str]:
                name = item[0]
                try:
                    return (0, f"{int(name):020d}")
                except ValueError:
                    return (1, name)

            files.sort(key=sort_key)

            to_delete = len(files) - max_files
            deleted = 0
            for _name, full_path in files[:to_delete]:
                try:
                    file_client = fs.get_file_client(full_path)
                    file_client.delete_file()
                    deleted += 1
                except Exception:
                    log.warning(f"Failed to delete source file: {full_path}")

            log.debug(
                f"cleanup_sources: deleted {deleted} files (was {len(files)}, limit {max_files})"
            )
            return deleted

        try:
            return self._retry(_cleanup, label=f"checkpoint.cleanup_sources {delta_location}")
        except Exception:
            log.warning(f"cleanup_sources failed for {delta_location}")
            return 0

    def delete_all_sources(self, delta_location: str) -> None:
        """Delete all files in ``_checkpoint/sources/`` (for full refresh)."""
        parsed = AbfssLocation.parse(delta_location)
        if parsed is None:
            return

        def _delete_all() -> None:
            service = _get_service(parsed, self._credential)
            fs = service.get_file_system_client(parsed.container)
            sources_dir = f"{parsed.path.rstrip('/')}/{_CHECKPOINT_DIR}/{_SOURCES_DIR}"

            deleted = 0
            for path_info in fs.get_paths(path=sources_dir, recursive=False):
                if getattr(path_info, "is_directory", False):
                    continue
                try:
                    file_client = fs.get_file_client(path_info.name)
                    file_client.delete_file()
                    deleted += 1
                except Exception:
                    pass
            log.debug(f"delete_all_sources: deleted {deleted} files for {delta_location}")

        try:
            self._retry(_delete_all, label=f"checkpoint.delete_all_sources {delta_location}")
        except Exception:
            log.debug(f"No sources to delete for {delta_location} (already clean)")

    def list_source_files(self, delta_location: str) -> list[str]:
        """List all file names in ``_checkpoint/sources/`` (for testing)."""
        parsed = AbfssLocation.parse(delta_location)
        if parsed is None:
            return []

        def _list() -> list[str]:
            service = _get_service(parsed, self._credential)
            fs = service.get_file_system_client(parsed.container)
            sources_dir = f"{parsed.path.rstrip('/')}/{_CHECKPOINT_DIR}/{_SOURCES_DIR}"

            names: list[str] = []
            for path_info in fs.get_paths(path=sources_dir, recursive=False):
                if getattr(path_info, "is_directory", False):
                    continue
                names.append(path_info.name.rsplit("/", 1)[-1])
            return sorted(names)

        try:
            return self._retry(_list, label=f"checkpoint.list_source_files {delta_location}")
        except Exception:
            return []

    def read_batch_source(self, delta_location: str, batch_id: int) -> list[dict]:
        """Read a batch's source records — tries JSONL first, then parquet snapshot."""
        parsed = AbfssLocation.parse(delta_location)
        if parsed is None:
            return []

        try:
            service = _get_service(parsed, self._credential)
            fs = service.get_file_system_client(parsed.container)
            sources_dir = f"{parsed.path.rstrip('/')}/{_CHECKPOINT_DIR}/{_SOURCES_DIR}"

            def _read_jsonl() -> list[dict]:
                jsonl_path = f"{sources_dir}/{batch_id}"
                file_client = fs.get_file_client(jsonl_path)
                raw = file_client.download_file().readall().decode("utf-8")
                return [json.loads(line) for line in raw.strip().split("\n") if line.strip()]

            try:
                return self._retry(
                    _read_jsonl,
                    label=f"checkpoint.read_batch_source jsonl batch={batch_id}",
                )
            except Exception:
                pass

            def _read_parquet() -> list[dict]:
                import os

                import duckdb

                parquet_path = f"{sources_dir}/{batch_id}.parquet"
                file_client = fs.get_file_client(parquet_path)
                parquet_bytes = file_client.download_file().readall()
                tmp_path = f"/tmp/dbt_scope_read_batch_{batch_id}.parquet"
                with open(tmp_path, "wb") as f:
                    f.write(parquet_bytes)
                conn = duckdb.connect()
                try:
                    rows = conn.execute(f"SELECT * FROM read_parquet('{tmp_path}')").fetchall()
                    cols = [
                        d[0]
                        for d in conn.execute(
                            f"DESCRIBE SELECT * FROM read_parquet('{tmp_path}')"
                        ).fetchall()
                    ]
                    all_records = [dict(zip(cols, row, strict=False)) for row in rows]
                    return [r for r in all_records if r.get("batchId") == batch_id]
                finally:
                    conn.close()
                    os.remove(tmp_path)

            try:
                return self._retry(
                    _read_parquet,
                    label=f"checkpoint.read_batch_source parquet batch={batch_id}",
                )
            except Exception:
                pass

            return []
        except Exception:
            return []
