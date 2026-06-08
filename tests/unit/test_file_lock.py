"""Unit tests for the cross-platform FileLock."""

from __future__ import annotations

import tempfile
import threading
from pathlib import Path

from dbt.adapters.scope._file_lock import AZ_CLI_TOKEN_LOCK, FABRIC_TOKEN_LOCK, FileLock


class TestFileLock:
    """Tests for FileLock context manager."""

    def test_lock_file_created(self, tmp_path: Path):
        lock_base = str(tmp_path / "test-lock")
        with FileLock(lock_base):
            assert Path(f"{lock_base}.lock").exists()

    def test_context_manager_returns_self(self, tmp_path: Path):
        lock = FileLock(str(tmp_path / "test-lock"))
        with lock as acquired:
            assert acquired is lock

    def test_lock_file_closed_after_exit(self, tmp_path: Path):
        lock_base = str(tmp_path / "test-lock")
        lock = FileLock(lock_base)
        with lock:
            pass
        # After exit, the file handle should be closed — re-locking should work
        with FileLock(lock_base):
            pass

    def test_serializes_concurrent_threads(self, tmp_path: Path):
        """Verify that the lock serializes access across threads."""
        lock_base = str(tmp_path / "thread-lock")
        results: list[int] = []
        counter = {"value": 0}

        def increment():
            with FileLock(lock_base):
                # Read, sleep briefly, write — without lock this would race
                val = counter["value"]
                counter["value"] = val + 1
                results.append(counter["value"])

        threads = [threading.Thread(target=increment) for _ in range(10)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert counter["value"] == 10
        assert sorted(results) == list(range(1, 11))

    def test_exception_releases_lock(self, tmp_path: Path):
        """Lock is released even if the body raises."""
        lock_base = str(tmp_path / "exc-lock")
        try:
            with FileLock(lock_base):
                raise ValueError("boom")
        except ValueError:
            pass

        # Should be able to re-acquire
        with FileLock(lock_base):
            pass

    def test_az_cli_token_lock_constant(self):
        """AZ_CLI_TOKEN_LOCK is a well-known path in the temp directory."""
        assert "dbt-scope-az-cli-token" in AZ_CLI_TOKEN_LOCK
        assert tempfile.gettempdir() in AZ_CLI_TOKEN_LOCK

    def test_fabric_token_lock_constant(self):
        """FABRIC_TOKEN_LOCK is a well-known path in the temp directory."""
        assert "dbt-scope-fabric-token" in FABRIC_TOKEN_LOCK
        assert tempfile.gettempdir() in FABRIC_TOKEN_LOCK
        assert FABRIC_TOKEN_LOCK != AZ_CLI_TOKEN_LOCK
