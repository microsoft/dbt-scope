"""Cross-platform file locking for serializing OS-level operations."""

from __future__ import annotations

import sys
import tempfile
from pathlib import Path
from types import TracebackType
from typing import TypeVar

T = TypeVar("T")

# Well-known lock file for Azure CLI token serialization
AZ_CLI_TOKEN_LOCK = str(Path(tempfile.gettempdir()) / "dbt-scope-az-cli-token")


class FileLock:
    """OS-level file lock context manager.

    Uses ``msvcrt.locking`` on Windows and ``fcntl.flock`` on Unix to provide
    cross-process mutual exclusion.  This is needed when multiple pytest-xdist
    workers (separate processes) call ``az account get-access-token`` at the
    same time — the Azure CLI token cache is not safe for concurrent access.

    Usage::

        with FileLock("/tmp/my-resource"):
            do_something_exclusively()
    """

    def __init__(self, lock_file: str) -> None:
        self.lock_path = Path(f"{lock_file}.lock")
        self._lock_file: object | None = None

    def __enter__(self) -> FileLock:
        self.lock_path.parent.mkdir(parents=True, exist_ok=True)
        self._lock_file = self.lock_path.open("w")
        if sys.platform == "win32":
            import msvcrt

            msvcrt.locking(self._lock_file.fileno(), msvcrt.LK_LOCK, 1)  # type: ignore[union-attr]
        else:
            import fcntl

            fcntl.flock(self._lock_file.fileno(), fcntl.LOCK_EX)  # type: ignore[union-attr]
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_val: BaseException | None,
        exc_tb: TracebackType | None,
    ) -> bool:
        if self._lock_file:
            if sys.platform == "win32":
                import msvcrt

                msvcrt.locking(self._lock_file.fileno(), msvcrt.LK_UNLCK, 1)  # type: ignore[union-attr]
            else:
                import fcntl

                fcntl.flock(self._lock_file.fileno(), fcntl.LOCK_UN)  # type: ignore[union-attr]
            self._lock_file.close()  # type: ignore[union-attr]
        return False
