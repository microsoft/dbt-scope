"""Tests for dynamic ADLA job naming via set_next_job_name."""

from __future__ import annotations

from contextlib import contextmanager
from unittest.mock import MagicMock, patch

import pytest

from dbt.adapters.scope.connections import ScopeConnectionHandle, ScopeConnectionManager

# A minimal non-comment SCOPE script that won't be skipped by execute()
_DUMMY_SCRIPT = '// SCOPE script\nSET @@FeaturePreviews = "EnableDeltaTableDynamicInsert:on";'


class TestNextJobNameOnHandle:
    """ScopeConnectionHandle._next_job_name is initialized and consumable."""

    def test_defaults_to_none(self):
        creds = MagicMock()
        creds.adla_account = "test"
        creds.http_timeout_seconds = 30
        creds.http_retries = 3
        with patch.object(ScopeConnectionHandle, "_build_session", return_value=MagicMock()):
            handle = ScopeConnectionHandle(creds)
        assert handle._next_job_name is None

    def test_can_be_set_and_read(self):
        creds = MagicMock()
        creds.adla_account = "test"
        creds.http_timeout_seconds = 30
        creds.http_retries = 3
        with patch.object(ScopeConnectionHandle, "_build_session", return_value=MagicMock()):
            handle = ScopeConnectionHandle(creds)
        handle._next_job_name = "events_daily_2026-04-01_2026-04-02"
        assert handle._next_job_name == "events_daily_2026-04-01_2026-04-02"


class TestExecuteJobName:
    """ScopeConnectionManager.execute() uses _next_job_name when set."""

    @pytest.fixture
    def mock_manager(self):
        """Create a ScopeConnectionManager with mocked internals."""
        mgr = MagicMock(spec=ScopeConnectionManager)
        mgr.execute = ScopeConnectionManager.execute.__get__(mgr, ScopeConnectionManager)

        @contextmanager
        def _exception_handler(sql):
            yield

        mgr.exception_handler = _exception_handler

        handle = MagicMock()
        handle._next_job_name = None
        handle.submit_and_wait = MagicMock()
        handle.submit_and_wait.return_value = MagicMock(job_id="test-id", result="Succeeded")

        connection = MagicMock()
        connection.handle = handle
        connection.credentials = MagicMock(
            au=100, priority=1, poll_interval_seconds=5, max_wait_seconds=60
        )
        mgr.get_thread_connection.return_value = connection
        return mgr, handle

    def test_uses_next_job_name_when_set(self, mock_manager):
        mgr, handle = mock_manager
        handle._next_job_name = "events_daily_full-refresh"

        mgr.execute(_DUMMY_SCRIPT)

        handle.submit_and_wait.assert_called_once()
        call_kwargs = handle.submit_and_wait.call_args
        assert call_kwargs.kwargs["name"] == "events_daily_full-refresh"

    def test_clears_next_job_name_after_use(self, mock_manager):
        mgr, handle = mock_manager
        handle._next_job_name = "events_daily_2026-04-01_2026-04-02"

        mgr.execute(_DUMMY_SCRIPT)

        assert handle._next_job_name is None

    def test_falls_back_to_default_when_not_set(self, mock_manager):
        mgr, handle = mock_manager
        handle._next_job_name = None

        mgr.execute(_DUMMY_SCRIPT)

        handle.submit_and_wait.assert_called_once()
        call_kwargs = handle.submit_and_wait.call_args
        assert call_kwargs.kwargs["name"] == "dbt-scope"

    def test_not_consumed_for_skipped_scripts(self, mock_manager):
        mgr, handle = mock_manager
        handle._next_job_name = "events_daily_full-refresh"

        # Comment-only script is skipped
        mgr.execute("-- no-op: full refresh already loaded")

        handle.submit_and_wait.assert_not_called()
        # Name should still be set since execute returned early
        assert handle._next_job_name == "events_daily_full-refresh"

    def test_explicit_job_name_kwarg_used_when_no_next_name(self, mock_manager):
        mgr, handle = mock_manager
        handle._next_job_name = None

        mgr.execute(_DUMMY_SCRIPT, job_name="custom-name")

        handle.submit_and_wait.assert_called_once()
        call_kwargs = handle.submit_and_wait.call_args
        assert call_kwargs.kwargs["name"] == "custom-name"

    def test_next_job_name_takes_precedence_over_default(self, mock_manager):
        mgr, handle = mock_manager
        handle._next_job_name = "from_macro"

        mgr.execute(_DUMMY_SCRIPT, job_name="dbt-scope")

        handle.submit_and_wait.assert_called_once()
        call_kwargs = handle.submit_and_wait.call_args
        assert call_kwargs.kwargs["name"] == "from_macro"
