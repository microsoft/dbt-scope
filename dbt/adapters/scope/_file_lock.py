"""Cross-platform file locking for serializing OS-level operations."""

from __future__ import annotations

import random
import sys
import tempfile
import time
from pathlib import Path
from types import TracebackType
from typing import TypeVar

from dbt.adapters.events.logging import AdapterLogger

log = AdapterLogger("scope")

T = TypeVar("T")

# Well-known lock file for Azure CLI token serialization
AZ_CLI_TOKEN_LOCK = str(Path(tempfile.gettempdir()) / "dbt-scope-az-cli-token")
# Well-known lock file for custom (e.g. Fabric notebook / SNI) token credentials
FABRIC_TOKEN_LOCK = str(Path(tempfile.gettempdir()) / "dbt-scope-fabric-token")

# Default timeout for acquiring the lock (seconds).  With several xdist workers
# all racing for the Azure CLI token at startup, contention is high.
_DEFAULT_TIMEOUT = 120


class FileLock:
    """OS-level file lock context manager.

    Uses ``msvcrt.locking`` on Windows and ``fcntl.flock`` on Unix to provide
    cross-process mutual exclusion.  This is needed when multiple pytest-xdist
    workers (separate processes) call ``az account get-access-token`` at the
    same time — the Azure CLI token cache is not safe for concurrent access.

    On Windows, ``msvcrt.LK_NBLCK`` is used with a manual retry loop and
    exponential backoff + jitter so that high-contention scenarios (24+
    workers) don't fail with ``OSError: Resource deadlock avoided``.

    Usage::

        with FileLock("/tmp/my-resource"):
            do_something_exclusively()
    """

    def __init__(self, lock_file: str, timeout: float = _DEFAULT_TIMEOUT) -> None:
        self.lock_path = Path(f"{lock_file}.lock")
        self._timeout = timeout
        self._lock_file: object | None = None

    def __enter__(self) -> FileLock:
        self.lock_path.parent.mkdir(parents=True, exist_ok=True)
        self._lock_file = self.lock_path.open("w")
        log.debug(f"Acquiring file lock: {self.lock_path!s}")
        if sys.platform == "win32":
            self._lock_win32()
        else:
            import fcntl

            fcntl.flock(self._lock_file.fileno(), fcntl.LOCK_EX)  # type: ignore[union-attr]
        log.debug(f"Acquired file lock: {self.lock_path!s}")
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
            log.debug(f"Released file lock: {self.lock_path!s}")
        return False

    # -- Windows retry logic --------------------------------------------------

    def _lock_win32(self) -> None:
        """Acquire the lock on Windows with retry + exponential backoff.

        ``msvcrt.LK_NBLCK`` returns immediately with an error if the lock
        is held, giving us full control over retry timing and timeout.
        """
        import msvcrt

        fd = self._lock_file.fileno()  # type: ignore[union-attr]
        deadline = time.monotonic() + self._timeout
        delay = 0.05  # initial backoff 50 ms
        attempt = 0

        while True:
            try:
                msvcrt.locking(fd, msvcrt.LK_NBLCK, 1)
                return
            except OSError:
                attempt += 1
                if time.monotonic() >= deadline:
                    raise OSError(
                        f"Could not acquire file lock {self.lock_path} "
                        f"after {self._timeout}s ({attempt} attempts)"
                    ) from None
                # Exponential backoff with jitter, capped at 2 s
                sleep_time = min(delay * (1 + random.random()), 2.0)
                if attempt % 20 == 0:
                    log.debug(
                        f"File lock contention on {self.lock_path!s}, "
                        f"attempt {attempt}, sleeping {sleep_time:.2f}s"
                    )
                time.sleep(sleep_time)
                delay = min(delay * 1.5, 2.0)
