"""Checkpoint manager — watermark + sources persistence for file-based processing.

Reads and writes ``_checkpoint/watermark.json`` and per-batch JSONL files in
``_checkpoint/sources/`` alongside ``_delta_log/`` in the Delta table root on
ADLS Gen2.

Watermark schema::

    {
        "version": 0,
        "modifiedTime": "2026-04-01T12:34:56.789000+00:00",
        "batchId": 3
    }

Sources JSONL (one line per processed file)::

    {"path": "/local/.../file.ss", "modificationTime": 1775018672000, "batchId": 0, "batchProcessingTime": "2026-04-06T21:00:00+00:00"}
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from datetime import datetime, timezone

from azure.identity import AzureCliCredential
from azure.storage.filedatalake import DataLakeServiceClient

from dbt.adapters.scope._file_lock import AZ_CLI_TOKEN_LOCK
from dbt.adapters.scope.delta_lake import AbfssLocation, LockedTokenCredential

log = logging.getLogger(__name__)

_CHECKPOINT_DIR = "_checkpoint"
_WATERMARK_FILE = "watermark.json"
_SOURCES_DIR = "sources"

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

    @property
    def modified_time_dt(self) -> datetime | None:
        """Parse ``modified_time`` to a timezone-aware ``datetime``, or None."""
        if not self.modified_time:
            return None
        return datetime.fromisoformat(self.modified_time)

    def to_json(self) -> str:
        return json.dumps(
            {
                "version": self.version,
                "modifiedTime": self.modified_time,
                "batchId": self.batch_id,
            },
            indent=2,
        )

    @classmethod
    def from_json(cls, raw: str) -> Watermark:
        data = json.loads(raw)
        return cls(
            version=data.get("version", 0),
            modified_time=data.get("modifiedTime", ""),
            batch_id=data.get("batchId", 0),
        )


def _get_service(parsed: AbfssLocation, credential: LockedTokenCredential) -> DataLakeServiceClient:
    return DataLakeServiceClient(account_url=parsed.account_url, credential=credential)


class CheckpointManager:
    """Manage ``_checkpoint/`` on ADLS Gen2 Delta table roots."""

    def __init__(self, *, lock_file: str = AZ_CLI_TOKEN_LOCK) -> None:
        self._credential = LockedTokenCredential(AzureCliCredential(), lock_file=lock_file)

    # -- Watermark ---------------------------------------------------------

    def read_watermark(self, delta_location: str) -> Watermark | None:
        """Read the watermark from ``_checkpoint/watermark.json``."""
        parsed = AbfssLocation.parse(delta_location)
        if parsed is None:
            log.warning("read_watermark: invalid delta_location: %s", delta_location)
            return None

        try:
            service = _get_service(parsed, self._credential)
            fs = service.get_file_system_client(parsed.container)
            file_path = f"{parsed.path.rstrip('/')}/{_CHECKPOINT_DIR}/{_WATERMARK_FILE}"
            file_client = fs.get_file_client(file_path)
            download = file_client.download_file()
            raw = download.readall().decode("utf-8")
            watermark = Watermark.from_json(raw)
            log.info(
                "Read watermark: version=%d, modified_time=%s, batch_id=%d",
                watermark.version,
                watermark.modified_time,
                watermark.batch_id,
            )
            return watermark
        except Exception:
            log.debug("No checkpoint found for %s (first run or full refresh)", delta_location)
            return None

    def write_watermark(self, delta_location: str, watermark: Watermark) -> None:
        """Write (create or overwrite) the watermark checkpoint."""
        parsed = AbfssLocation.parse(delta_location)
        if parsed is None:
            log.warning("write_watermark: invalid delta_location: %s", delta_location)
            return

        try:
            service = _get_service(parsed, self._credential)
            fs = service.get_file_system_client(parsed.container)

            dir_path = f"{parsed.path.rstrip('/')}/{_CHECKPOINT_DIR}"
            dir_client = fs.get_directory_client(dir_path)
            dir_client.create_directory()

            file_path = f"{dir_path}/{_WATERMARK_FILE}"
            file_client = fs.get_file_client(file_path)
            data = watermark.to_json().encode("utf-8")
            file_client.upload_data(data, overwrite=True)

            log.info(
                "Wrote watermark: version=%d, modified_time=%s, batch_id=%d → %s",
                watermark.version,
                watermark.modified_time,
                watermark.batch_id,
                delta_location,
            )
        except Exception:
            log.error("write_watermark failed for %s", delta_location, exc_info=True)
            raise

    def delete_watermark(self, delta_location: str) -> None:
        """Delete the watermark checkpoint and all sources (for full refresh)."""
        parsed = AbfssLocation.parse(delta_location)
        if parsed is None:
            log.warning("delete_watermark: invalid delta_location: %s", delta_location)
            return

        try:
            service = _get_service(parsed, self._credential)
            fs = service.get_file_system_client(parsed.container)
            file_path = f"{parsed.path.rstrip('/')}/{_CHECKPOINT_DIR}/{_WATERMARK_FILE}"
            file_client = fs.get_file_client(file_path)
            file_client.delete_file()
            log.info("Deleted watermark for %s", delta_location)
        except Exception:
            log.debug("No watermark to delete for %s (already clean)", delta_location)

        # Also delete all sources
        self.delete_all_sources(delta_location)

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
        written containing ALL history (previous snapshot + JSONL diffs +
        this batch).  Otherwise a JSONL diff is written.

        Layout::

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

        try:
            service = _get_service(parsed, self._credential)
            fs = service.get_file_system_client(parsed.container)
            sources_dir = f"{parsed.path.rstrip('/')}/{_CHECKPOINT_DIR}/{_SOURCES_DIR}"
            dir_client = fs.get_directory_client(sources_dir)
            dir_client.create_directory()

            if is_compaction:
                self._write_snapshot_parquet(fs, sources_dir, batch_id, batch_records)
            else:
                self._write_jsonl(fs, sources_dir, batch_id, batch_records)

            log.info(
                "Wrote sources batch %d (%d files, %s) → %s",
                batch_id,
                len(file_paths),
                "parquet snapshot" if is_compaction else "jsonl diff",
                delta_location,
            )
        except Exception:
            log.error("write_batch_sources failed for batch %d", batch_id, exc_info=True)
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
        lines = [json.dumps(r, separators=(",", ":")) for r in records]
        content = "\n".join(lines)
        file_path = f"{sources_dir}/{batch_id}"
        file_client = fs.get_file_client(file_path)
        file_client.upload_data(content.encode("utf-8"), overwrite=True)

    def _write_snapshot_parquet(
        self, fs, sources_dir: str, batch_id: int, current_batch_records: list[dict]
    ) -> None:
        """Write a full-history parquet snapshot.

        Reads the most recent parquet snapshot (if any) + all JSONL diffs
        since that snapshot, UNIONs them with the current batch records,
        and writes ``{batch_id}.parquet``.
        """
        import os

        import duckdb

        # Collect all history: previous snapshot + JSONL diffs
        all_records: list[dict] = []
        snapshot_batch_id: int = -1

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
                    snapshot_batch_id = max(snapshot_batch_id, snap_id)
                except ValueError:
                    pass

        for name, full_path in file_entries:
            if name.endswith(".parquet"):
                # Read previous snapshot
                try:
                    file_client = fs.get_file_client(full_path)
                    parquet_bytes = file_client.download_file().readall()
                    tmp_path = f"/tmp/dbt_scope_read_{name}"
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
                    log.warning("Failed to read snapshot %s", name, exc_info=True)
                continue

            # JSONL diff files (numeric names, no extension)
            try:
                jsonl_batch_id = int(name)
            except ValueError:
                continue

            # Skip JSONL diffs already folded into the latest parquet snapshot
            if jsonl_batch_id <= snapshot_batch_id:
                continue

            try:
                file_client = fs.get_file_client(full_path)
                raw = file_client.download_file().readall().decode("utf-8")
                for line in raw.strip().split("\n"):
                    if line.strip():
                        all_records.append(json.loads(line))
            except Exception:
                log.warning("Failed to read JSONL %s", name, exc_info=True)

        # Add current batch records
        all_records.extend(current_batch_records)

        # Write consolidated parquet via DuckDB (NDJSON → read_json_auto → COPY)
        parquet_local = f"/tmp/dbt_scope_{batch_id}.parquet"
        ndjson_local = f"/tmp/dbt_scope_{batch_id}.ndjson"
        try:
            with open(ndjson_local, "w") as nf:
                for r in all_records:
                    nf.write(json.dumps(r) + "\n")

            conn = duckdb.connect()
            try:
                conn.execute(
                    f"CREATE TABLE sources AS SELECT * FROM read_json_auto('{ndjson_local}')"
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

        # Delete old parquet snapshots (replaced by new one)
        import contextlib

        for path_info in fs.get_paths(path=sources_dir, recursive=False):
            if getattr(path_info, "is_directory", False):
                continue
            name = path_info.name.rsplit("/", 1)[-1]
            if name.endswith(".parquet") and name != f"{batch_id}.parquet":
                with contextlib.suppress(Exception):
                    fs.get_file_client(path_info.name).delete_file()

        log.info(
            "Wrote snapshot %d.parquet (%d total records)",
            batch_id,
            len(all_records),
        )

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

        try:
            service = _get_service(parsed, self._credential)
            fs = service.get_file_system_client(parsed.container)
            sources_dir = f"{parsed.path.rstrip('/')}/{_CHECKPOINT_DIR}/{_SOURCES_DIR}"

            # List all files
            files: list[tuple[str, str]] = []  # (name, full_path)
            for path_info in fs.get_paths(path=sources_dir, recursive=False):
                if getattr(path_info, "is_directory", False):
                    continue
                name = path_info.name.rsplit("/", 1)[-1]
                files.append((name, path_info.name))

            if len(files) <= max_files:
                return 0

            # Sort: JSONL files (numeric names) first by batch_id, then parquet by name
            def sort_key(item: tuple[str, str]) -> tuple[int, str]:
                name = item[0]
                try:
                    return (0, f"{int(name):020d}")
                except ValueError:
                    return (1, name)

            files.sort(key=sort_key)

            # Delete oldest files until we're at the limit
            to_delete = len(files) - max_files
            deleted = 0
            for _name, full_path in files[:to_delete]:
                try:
                    file_client = fs.get_file_client(full_path)
                    file_client.delete_file()
                    deleted += 1
                except Exception:
                    log.warning("Failed to delete source file: %s", full_path, exc_info=True)

            log.info(
                "cleanup_sources: deleted %d files (was %d, limit %d)",
                deleted,
                len(files),
                max_files,
            )
            return deleted
        except Exception:
            log.warning("cleanup_sources failed for %s", delta_location, exc_info=True)
            return 0

    def delete_all_sources(self, delta_location: str) -> None:
        """Delete all files in ``_checkpoint/sources/`` (for full refresh)."""
        parsed = AbfssLocation.parse(delta_location)
        if parsed is None:
            return

        try:
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
            log.info("delete_all_sources: deleted %d files for %s", deleted, delta_location)
        except Exception:
            log.debug("No sources to delete for %s (already clean)", delta_location)

    def list_source_files(self, delta_location: str) -> list[str]:
        """List all file names in ``_checkpoint/sources/`` (for testing)."""
        parsed = AbfssLocation.parse(delta_location)
        if parsed is None:
            return []

        try:
            service = _get_service(parsed, self._credential)
            fs = service.get_file_system_client(parsed.container)
            sources_dir = f"{parsed.path.rstrip('/')}/{_CHECKPOINT_DIR}/{_SOURCES_DIR}"

            names: list[str] = []
            for path_info in fs.get_paths(path=sources_dir, recursive=False):
                if getattr(path_info, "is_directory", False):
                    continue
                names.append(path_info.name.rsplit("/", 1)[-1])
            return sorted(names)
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

            # Try JSONL first
            try:
                jsonl_path = f"{sources_dir}/{batch_id}"
                file_client = fs.get_file_client(jsonl_path)
                raw = file_client.download_file().readall().decode("utf-8")
                return [json.loads(line) for line in raw.strip().split("\n") if line.strip()]
            except Exception:
                pass

            # Try parquet snapshot (compaction batches)
            try:
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
                    # Filter to only records for this batch_id
                    all_records = [dict(zip(cols, row, strict=False)) for row in rows]
                    return [r for r in all_records if r.get("batchId") == batch_id]
                finally:
                    conn.close()
                    os.remove(tmp_path)
            except Exception:
                pass

            return []
        except Exception:
            return []
