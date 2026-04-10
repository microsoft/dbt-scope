"""ADLS Gen1 file listing client for dbt-scope.

Wraps ``azure.datalake.store`` to recursively list files on ADLS Gen1,
returning structured ``FileInfo`` objects with modification timestamps
used for watermark-based filtering.
"""

from __future__ import annotations

import logging
import re
import time
from concurrent.futures import FIRST_COMPLETED, Future, ThreadPoolExecutor, wait
from dataclasses import dataclass, field, replace
from datetime import datetime, timezone

from azure.datalake.store import core as adls_core
from azure.identity import AzureCliCredential
from dbt.adapters.events.logging import AdapterLogger

from dbt.adapters.scope._file_lock import AZ_CLI_TOKEN_LOCK, FileLock

log = AdapterLogger("scope")


class _SuppressFileNotFound(logging.Filter):
    """Reject Azure SDK log records that report a 404 / FileNotFoundError.

    The adapter already catches these exceptions and logs a concise debug
    message, so the SDK's verbose ERROR (full HTTP headers) is redundant.
    """

    _KEYWORDS = ("FileNotFoundError", "FileNotFoundException")

    def filter(self, record: logging.LogRecord) -> bool:
        msg = record.getMessage()
        return not any(kw in msg for kw in self._KEYWORDS)


logging.getLogger("azure.datalake.store").addFilter(_SuppressFileNotFound())
logging.getLogger("azure.datalake.store.lib").addFilter(_SuppressFileNotFound())


@dataclass(frozen=True)
class FileInfo:
    """Metadata for a single file on ADLS Gen1."""

    path: str
    name: str
    length: int
    modification_time: datetime
    raw: dict = field(default_factory=dict, repr=False, compare=False, hash=False)
    estimated_bytes: int | None = field(default=None, repr=False, compare=False, hash=False)
    contributing_files: tuple[str, ...] = field(default=(), repr=False, compare=False, hash=False)

    @classmethod
    def from_adls_entry(cls, entry: dict) -> FileInfo | None:
        """Build a ``FileInfo`` from an ADLS Gen1 listing entry.

        Returns ``None`` if the entry is a directory or has no modification time.
        """
        if entry.get("type") == "DIRECTORY":
            return None
        mod_ms = entry.get("modificationTime")
        if mod_ms is None:
            return None
        # ADLS Gen1 SDK returns paths without leading / — normalize for SCOPE
        raw_path = entry["name"]
        path = raw_path if raw_path.startswith("/") else f"/{raw_path}"
        return cls(
            path=path,
            name=raw_path.rsplit("/", 1)[-1],
            length=entry.get("length", 0),
            modification_time=datetime.fromtimestamp(mod_ms / 1000, tz=timezone.utc),
            raw=entry,
        )


def _list_one_dir(
    fs: adls_core.AzureDLFileSystem,
    dir_path: str,
    depth: int,
) -> tuple[list[dict], list[dict], str, int, float]:
    """List a single directory. Returns (files, subdirs, path, depth, elapsed_ms)."""
    t0 = time.monotonic()
    try:
        entries = fs.ls(dir_path, detail=True)
    except FileNotFoundError:
        log.debug(f"Path not found (skipping): {dir_path}")
        return [], [], dir_path, depth, (time.monotonic() - t0) * 1000
    except Exception:
        log.warning(f"Failed to list {dir_path} (skipping)")
        return [], [], dir_path, depth, (time.monotonic() - t0) * 1000

    elapsed_ms = (time.monotonic() - t0) * 1000
    files = [e for e in entries if e.get("type") != "DIRECTORY"]
    dirs = [e for e in entries if e.get("type") == "DIRECTORY"]
    return files, dirs, dir_path, depth, elapsed_ms


class AdlsGen1Client:
    """Client for listing files on ADLS Gen1 with watermark support."""

    def __init__(
        self,
        account: str,
        *,
        lock_file: str = AZ_CLI_TOKEN_LOCK,
    ) -> None:
        self._account = account
        self._lock_file = lock_file
        self._fs: adls_core.AzureDLFileSystem | None = None

    def _get_fs(self) -> adls_core.AzureDLFileSystem:
        """Lazily initialize the ADLS Gen1 filesystem client."""
        if self._fs is None:
            with FileLock(self._lock_file):
                credential = AzureCliCredential()
            self._fs = adls_core.AzureDLFileSystem(
                token_credential=credential,
                store_name=self._account,
            )
        return self._fs

    def list_files(
        self,
        root: str,
        *,
        pattern: str | None = None,
        recursive: bool = True,
        max_workers: int = 8,
    ) -> list[FileInfo]:
        """List all files under *root*, optionally filtering by regex *pattern*.

        Args:
            root: ADLS Gen1 path (e.g. ``/shares/SQLDB.Prod/local/...``).
            pattern: Regex pattern to match against the file name (not full path).
                     Only files whose name matches are returned.
            recursive: If True, walk subdirectories in parallel.
            max_workers: Max threads for parallel recursive listing.

        Returns:
            Sorted list of ``FileInfo`` objects (sorted by modification_time ASC).
        """
        fs = self._get_fs()
        compiled = re.compile(pattern) if pattern else None
        log.debug(f"Listing files: account={self._account}, root={root}, pattern={pattern}")

        walk_start = time.monotonic()
        if recursive:
            raw_entries = self._walk(fs, root, max_workers)
        else:
            t0 = time.monotonic()
            try:
                raw_entries = fs.ls(root, detail=True)
            except FileNotFoundError:
                log.debug(f"Path not found: {root}")
                return []
            except Exception:
                log.warning(f"Failed to list {root}")
                return []
            finally:
                elapsed_ms = (time.monotonic() - t0) * 1000
                log.debug(f"ls {root} completed in {elapsed_ms:.1f} ms")
        walk_elapsed_ms = (time.monotonic() - walk_start) * 1000
        log.debug(f"Total walk of {root} completed in {walk_elapsed_ms:.1f} ms")

        files: list[FileInfo] = []
        skipped_empty = 0
        for entry in raw_entries:
            info = FileInfo.from_adls_entry(entry)
            if info is None:
                continue
            if info.length == 0:
                skipped_empty += 1
                continue
            if compiled and not compiled.search(info.name):
                continue
            files.append(info)

        if skipped_empty:
            log.debug(f"Skipped {skipped_empty} zero-length files under {root}")
        files.sort(key=lambda f: f.modification_time)
        log.debug(f"Found {len(files)} files matching pattern under {root}")
        return files

    @staticmethod
    def _walk(
        fs: adls_core.AzureDLFileSystem,
        root: str,
        max_workers: int,
    ) -> list[dict]:
        """Walk directories in parallel, logging per-directory progress."""
        all_files: list[dict] = []
        dirs_done = 0

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures: dict[Future, tuple[str, int]] = {}

            f = executor.submit(_list_one_dir, fs, root, 0)
            futures[f] = (root, 0)

            while futures:
                done, _ = wait(futures, return_when=FIRST_COMPLETED)

                for completed in done:
                    futures.pop(completed)
                    try:
                        files, dirs, dir_path, depth, elapsed_ms = completed.result()
                    except Exception:
                        dirs_done += 1
                        continue

                    dirs_done += 1
                    short = dir_path.rsplit("/", 1)[-1] or dir_path
                    log.debug(
                        f"Depth {depth} | {short} → "
                        f"{len(dirs)} dirs, {len(files)} files "
                        f"({elapsed_ms:.0f} ms) | "
                        f"done: {dirs_done}, in-flight: {len(futures)}"
                    )

                    all_files.extend(files)

                    for d in sorted(dirs, key=lambda e: e.get("name", "")):
                        new_f = executor.submit(_list_one_dir, fs, d["name"], depth + 1)
                        futures[new_f] = (d["name"], depth + 1)

                if futures:
                    log.debug(f"Queue: {len(futures)} directories pending")

        log.debug(f"Walk complete: {dirs_done} directories scanned")
        return all_files

    # ------------------------------------------------------------------
    # Byte estimation for SSv5/v6 structured streams
    # ------------------------------------------------------------------

    def estimate_bytes(self, ss_path: str, ss_length: int) -> tuple[int, list[str]]:
        """Estimate total data size of a structured stream file.

        For SSv3/v4 (no sibling folder), the file size IS the data size.
        For SSv5/v6, a sibling folder contains ``.du`` data files whose
        sizes must be summed with the manifest.

        Args:
            ss_path: Full ADLS Gen1 path to a ``.ss`` file.
            ss_length: Known file size of the ``.ss`` file (avoids re-fetch).

        Returns:
            Tuple of ``(estimated_total_bytes, contributing_file_paths)``.
            ``contributing_file_paths`` is empty for SSv3/v4.
        """
        if not ss_path.endswith(".ss"):
            raise ValueError(f"Path must end with '.ss', got: {ss_path}")

        sibling_folder = ss_path[:-3]  # strip ".ss"
        fs = self._get_fs()

        if not self._directory_exists(sibling_folder, fs):
            return ss_length, []

        files_in_folder = self._list_directory_files(sibling_folder, fs)
        folder_total = sum(f["length"] for f in files_in_folder)
        contributing_paths = [f["name"] for f in files_in_folder]

        log.debug(
            f"estimate_bytes: {ss_path.rsplit('/', 1)[-1]} → SSv5/v6 "
            f"({len(contributing_paths)} contributing files, "
            f"{_format_bytes_simple(folder_total)} folder + "
            f"{_format_bytes_simple(ss_length)} manifest)"
        )
        return ss_length + folder_total, contributing_paths

    def enrich_with_estimates(self, files: list[FileInfo]) -> list[FileInfo]:
        """Enrich a list of ``FileInfo`` objects with byte estimates.

        Calls :meth:`estimate_bytes` for each file and returns new
        ``FileInfo`` instances with ``estimated_bytes`` and
        ``contributing_files`` populated.
        """
        if not files:
            return files

        t0 = time.monotonic()
        enriched: list[FileInfo] = []
        for f in files:
            try:
                est_bytes, contrib_paths = self.estimate_bytes(f.path, f.length)
                enriched.append(
                    replace(
                        f,
                        estimated_bytes=est_bytes,
                        contributing_files=tuple(contrib_paths),
                    )
                )
            except Exception:
                log.warning(f"Failed to estimate bytes for {f.path} — using file length")
                enriched.append(replace(f, estimated_bytes=f.length))

        elapsed_ms = (time.monotonic() - t0) * 1000
        log.debug(f"enrich_with_estimates: {len(enriched)} files enriched in {elapsed_ms:.1f} ms")
        return enriched

    @staticmethod
    def _directory_exists(path: str, fs: adls_core.AzureDLFileSystem) -> bool:
        """Check if a directory exists on ADLS Gen1."""
        try:
            info = fs.info(path)
            return info.get("type") == "DIRECTORY"
        except FileNotFoundError:
            return False
        except Exception:
            log.debug(f"_directory_exists: error checking {path} — assuming not exists")
            return False

    @staticmethod
    def _list_directory_files(
        dir_path: str,
        fs: adls_core.AzureDLFileSystem,
    ) -> list[dict]:
        """Recursively list all files in a directory, returning raw entries."""
        all_files: list[dict] = []
        dirs_to_visit = [dir_path]

        while dirs_to_visit:
            current = dirs_to_visit.pop()
            try:
                entries = fs.ls(current, detail=True)
            except FileNotFoundError:
                continue
            except Exception:
                log.debug(f"_list_directory_files: failed to list {current} — skipping")
                continue

            for entry in entries:
                if entry.get("type") == "DIRECTORY":
                    dirs_to_visit.append(entry["name"])
                else:
                    all_files.append(entry)

        return all_files


def _format_bytes_simple(size: int) -> str:
    """Compact human-readable byte size (used in log messages)."""
    s = float(size)
    for unit in ("B", "KB", "MB", "GB", "TB"):
        if abs(s) < 1024:
            return f"{s:.1f} {unit}"
        s /= 1024
    return f"{s:.1f} PB"
