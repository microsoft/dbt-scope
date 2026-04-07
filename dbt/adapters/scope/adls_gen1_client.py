"""ADLS Gen1 file listing client for dbt-scope.

Wraps ``azure.datalake.store`` to recursively list files on ADLS Gen1,
returning structured ``FileInfo`` objects with modification timestamps
used for watermark-based filtering.
"""

from __future__ import annotations

import logging
import re
from dataclasses import dataclass
from datetime import datetime, timezone

from azure.datalake.store import core as adls_core
from azure.identity import AzureCliCredential

from dbt.adapters.scope._file_lock import AZ_CLI_TOKEN_LOCK, FileLock

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class FileInfo:
    """Metadata for a single file on ADLS Gen1."""

    path: str
    name: str
    length: int
    modification_time: datetime

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
        )


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
    ) -> list[FileInfo]:
        """List all files under *root*, optionally filtering by regex *pattern*.

        Args:
            root: ADLS Gen1 path (e.g. ``/shares/SQLDB.Prod/local/...``).
            pattern: Regex pattern to match against the file name (not full path).
                     Only files whose name matches are returned.
            recursive: If True, walk subdirectories.

        Returns:
            Sorted list of ``FileInfo`` objects (sorted by modification_time ASC).
        """
        fs = self._get_fs()
        compiled = re.compile(pattern) if pattern else None
        log.info("Listing files: account=%s, root=%s, pattern=%s", self._account, root, pattern)

        if recursive:
            raw_entries = self._walk(fs, root)
        else:
            try:
                raw_entries = fs.ls(root, detail=True)
            except FileNotFoundError:
                log.warning("Path not found: %s", root)
                return []
            except Exception:
                log.warning("Failed to list %s", root, exc_info=True)
                return []

        files: list[FileInfo] = []
        for entry in raw_entries:
            info = FileInfo.from_adls_entry(entry)
            if info is None:
                continue
            if compiled and not compiled.search(info.name):
                continue
            files.append(info)

        files.sort(key=lambda f: f.modification_time)
        log.info("Found %d files matching pattern under %s", len(files), root)
        return files

    @staticmethod
    def _walk(
        fs: adls_core.AzureDLFileSystem,
        path: str,
    ) -> list[dict]:
        """Recursively list all file entries under *path*."""
        try:
            entries = fs.ls(path, detail=True)
        except FileNotFoundError:
            log.warning("Path not found (skipping): %s", path)
            return []
        except Exception:
            log.warning("Failed to list %s (skipping)", path, exc_info=True)
            return []

        files = [e for e in entries if e.get("type") != "DIRECTORY"]
        dirs = [e for e in entries if e.get("type") == "DIRECTORY"]

        for d in sorted(dirs, key=lambda e: e.get("name", "")):
            files.extend(AdlsGen1Client._walk(fs, d["name"]))

        return files
