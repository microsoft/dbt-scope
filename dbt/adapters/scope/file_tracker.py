"""File tracker — discover unprocessed files and batch them for SCOPE jobs.

Orchestrates :class:`~dbt.adapters.scope.adls_gen1_client.AdlsGen1Client`
and :class:`~dbt.adapters.scope.checkpoint.CheckpointManager` to implement
the per-file processing loop:

1. LIST all files matching the regex pattern on ADLS Gen1
2. Filter out already-processed files (modificationTime <= watermark)
3. Apply safety buffer (skip files modified within last N seconds)
4. Return batches of up to ``max_files_per_trigger`` files
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

from dbt.adapters.events.logging import AdapterLogger

from dbt.adapters.scope.adls_gen1_client import AdlsGen1Client, FileInfo
from dbt.adapters.scope.checkpoint import CheckpointManager, Watermark

log = AdapterLogger("scope")

DEFAULT_SAFETY_BUFFER_SECONDS = 30
DEFAULT_MAX_FILES_PER_TRIGGER = 50


class FileTracker:
    """Discover and batch unprocessed source files for SCOPE ingestion."""

    def __init__(
        self,
        gen1_client: AdlsGen1Client,
        checkpoint_manager: CheckpointManager,
    ) -> None:
        self._gen1 = gen1_client
        self._checkpoint = checkpoint_manager

    def discover_unprocessed_files(
        self,
        root: str,
        pattern: str,
        watermark: Watermark | None,
        safety_buffer_seconds: int = DEFAULT_SAFETY_BUFFER_SECONDS,
    ) -> list[FileInfo]:
        """List all files under *root* matching *pattern* that are unprocessed.

        A file is "unprocessed" if:
          - Its ``modification_time`` is after the watermark's ``modified_time``
            (or there is no watermark — all files are unprocessed)
          - Its ``modification_time`` is before ``now - safety_buffer`` to avoid
            reading partially-written files

        Results are sorted by ``modification_time`` ascending (oldest first).
        """
        all_files = self._gen1.list_files(root, pattern=pattern)

        cutoff = datetime.now(timezone.utc) - timedelta(seconds=safety_buffer_seconds)
        watermark_dt = watermark.modified_time_dt if watermark else None

        unprocessed: list[FileInfo] = []
        for f in all_files:
            # Skip files that are too recent (safety buffer)
            if f.modification_time > cutoff:
                continue
            # Skip files already processed (at or before watermark)
            if watermark_dt and f.modification_time <= watermark_dt:
                continue
            unprocessed.append(f)

        log.debug(
            "Discovered %d unprocessed files (total=%d, watermark=%s, cutoff=%s)",
            len(unprocessed),
            len(all_files),
            watermark.modified_time if watermark else "none",
            cutoff.isoformat(),
        )
        return unprocessed

    @staticmethod
    def get_next_batch(
        files: list[FileInfo],
        max_files_per_trigger: int = DEFAULT_MAX_FILES_PER_TRIGGER,
    ) -> list[FileInfo]:
        """Take the first *max_files_per_trigger* files from the sorted list."""
        batch = files[:max_files_per_trigger]
        log.debug(
            "Next batch: %d files (of %d remaining)",
            len(batch),
            len(files),
        )
        return batch

    @staticmethod
    def compute_new_watermark(
        batch: list[FileInfo],
        current_watermark: Watermark | None,
    ) -> Watermark:
        """Compute the updated watermark after processing a batch.

        The new watermark's ``modified_time`` is the max modification time
        of the batch. The ``version`` is bumped by 1 from the current.
        The ``batch_id`` is also bumped by 1.
        """
        if not batch:
            if current_watermark:
                return current_watermark
            return Watermark()

        max_mod_time = max(f.modification_time for f in batch)
        version = (current_watermark.version + 1) if current_watermark else 0
        batch_id = (current_watermark.batch_id + 1) if current_watermark else 0

        return Watermark(
            version=version,
            modified_time=max_mod_time.isoformat(),
            batch_id=batch_id,
        )
