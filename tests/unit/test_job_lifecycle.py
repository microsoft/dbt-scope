"""Tests for ADLA job lifecycle: related metadata, list_jobs, and orphan cancellation."""

from __future__ import annotations

import uuid
from contextlib import contextmanager
from unittest.mock import MagicMock, call, patch
from urllib.parse import quote as url_quote

import pytest

from dbt.adapters.scope.connections import (
    _UUID_NAMESPACE,
    ScopeConnectionHandle,
    ScopeConnectionManager,
)

# A minimal non-comment SCOPE script that won't be skipped by execute()
_DUMMY_SCRIPT = '// SCOPE script\nSET @@FeaturePreviews = "EnableDeltaTableDynamicInsert:on";'


@pytest.fixture(autouse=True)
def _reset_class_state():
    """Reset class-level state between tests."""
    old_run_id = ScopeConnectionHandle._run_id
    old_cancelled = ScopeConnectionHandle._cancelled_models.copy()
    yield
    ScopeConnectionHandle._run_id = old_run_id
    ScopeConnectionHandle._cancelled_models = old_cancelled


def _make_handle(account: str = "test-adla") -> ScopeConnectionHandle:
    """Create a ScopeConnectionHandle with mocked internals."""
    creds = MagicMock()
    creds.adla_account = account
    creds.http_timeout_seconds = 30
    creds.http_retries = 3
    with patch.object(ScopeConnectionHandle, "_build_session", return_value=MagicMock()):
        return ScopeConnectionHandle(creds)


# =====================================================================
# Part A: related metadata
# =====================================================================


class TestRunId:
    """_run_id is a class-level UUID shared across all instances."""

    def test_is_valid_uuid(self):
        uuid.UUID(ScopeConnectionHandle._run_id)  # should not raise

    def test_same_across_instances(self):
        h1 = _make_handle("acct-a")
        h2 = _make_handle("acct-b")
        assert h1._run_id == h2._run_id
        assert h1._run_id is ScopeConnectionHandle._run_id


class TestPipelineId:
    """_pipeline_id is deterministic from the ADLA account name."""

    def test_deterministic(self):
        h1 = _make_handle("my-adla-account")
        h2 = _make_handle("my-adla-account")
        assert h1._pipeline_id == h2._pipeline_id

    def test_differs_for_different_accounts(self):
        h1 = _make_handle("account-a")
        h2 = _make_handle("account-b")
        assert h1._pipeline_id != h2._pipeline_id

    def test_is_uuid5_of_account(self):
        handle = _make_handle("my-adla")
        expected = str(uuid.uuid5(_UUID_NAMESPACE, "my-adla"))
        assert handle._pipeline_id == expected


class TestRecurrenceId:
    """recurrenceId in related metadata is deterministic from model name."""

    def test_deterministic_for_same_model(self):
        id1 = str(uuid.uuid5(_UUID_NAMESPACE, "events_daily"))
        id2 = str(uuid.uuid5(_UUID_NAMESPACE, "events_daily"))
        assert id1 == id2

    def test_differs_for_different_models(self):
        id1 = str(uuid.uuid5(_UUID_NAMESPACE, "events_daily"))
        id2 = str(uuid.uuid5(_UUID_NAMESPACE, "user_sessions"))
        assert id1 != id2


class TestSubmitJobRelatedMetadata:
    """submit_job includes related metadata when model_name is provided."""

    def test_includes_related_when_model_name_set(self):
        handle = _make_handle("test-adla")
        handle._request = MagicMock(return_value={"state": "Preparing"})

        handle.submit_job(
            name="events_daily_full-refresh_batch_1_of_1_files_5",
            script="// script",
            au=100,
            priority=1,
            model_name="events_daily",
        )

        call_kwargs = handle._request.call_args
        body = call_kwargs.kwargs["json"]
        assert "related" in body
        related = body["related"]
        assert related["pipelineId"] == handle._pipeline_id
        assert related["pipelineName"] == "dbt-scope"
        assert related["pipelineUri"] == "https://github.com/microsoft/dbt-scope"
        assert related["runId"] == ScopeConnectionHandle._run_id
        assert related["recurrenceId"] == str(uuid.uuid5(_UUID_NAMESPACE, "events_daily"))
        assert related["recurrenceName"] == "events_daily"

    def test_no_related_when_model_name_is_none(self):
        handle = _make_handle("test-adla")
        handle._request = MagicMock(return_value={"state": "Preparing"})

        handle.submit_job(
            name="dbt-scope",
            script="// script",
            au=100,
            priority=1,
            model_name=None,
        )

        body = handle._request.call_args.kwargs["json"]
        assert "related" not in body

    def test_no_related_when_model_name_not_provided(self):
        handle = _make_handle("test-adla")
        handle._request = MagicMock(return_value={"state": "Preparing"})

        handle.submit_job(
            name="dbt-scope",
            script="// script",
            au=100,
            priority=1,
        )

        body = handle._request.call_args.kwargs["json"]
        assert "related" not in body


# =====================================================================
# Part B: list_jobs and cancel_orphaned_jobs
# =====================================================================


class TestListJobs:
    """list_jobs builds correct ADLA API URLs."""

    def test_url_without_filter(self):
        handle = _make_handle()
        handle._request = MagicMock(return_value={"value": []})

        result = handle.list_jobs()

        assert result == []
        url = handle._request.call_args.args[1]
        assert "/Jobs?" in url
        assert "$top=100" in url
        assert "$filter" not in url

    def test_url_with_filter(self):
        handle = _make_handle()
        handle._request = MagicMock(return_value={"value": []})

        filter_expr = "startswith(name,'events_') and state ne 'Ended'"
        handle.list_jobs(filter_expr=filter_expr)

        url = handle._request.call_args.args[1]
        assert f"$filter={url_quote(filter_expr)}" in url

    def test_custom_top(self):
        handle = _make_handle()
        handle._request = MagicMock(return_value={"value": []})

        handle.list_jobs(top=50)

        url = handle._request.call_args.args[1]
        assert "$top=50" in url

    def test_returns_value_list(self):
        handle = _make_handle()
        jobs = [{"jobId": "j1", "name": "events_batch_1"}]
        handle._request = MagicMock(return_value={"value": jobs})

        result = handle.list_jobs()

        assert result == jobs


class TestCancelJob:
    """cancel_job polls until the job reaches a terminal state."""

    def test_polls_until_ended(self):
        handle = _make_handle()
        # POST (cancel) returns empty, then GET polls return Finalizing, then Ended
        handle._request = MagicMock(
            side_effect=[
                {},  # POST CancelJob
                {"state": "Finalizing", "result": None},  # GET poll 1
                {"state": "Ended", "result": "Cancelled"},  # GET poll 2
            ]
        )

        handle.cancel_job("job-123", poll_interval=0)

        assert handle._request.call_count == 3
        # First call is the POST cancel
        assert handle._request.call_args_list[0].args[0] == "POST"
        # Remaining calls are GET polls
        assert handle._request.call_args_list[1].args[0] == "GET"
        assert handle._request.call_args_list[2].args[0] == "GET"

    def test_returns_immediately_if_already_ended(self):
        handle = _make_handle()
        handle._request = MagicMock(
            side_effect=[
                {},  # POST CancelJob
                {"state": "Ended", "result": "Cancelled"},  # GET poll 1
            ]
        )

        handle.cancel_job("job-123", poll_interval=0)

        assert handle._request.call_count == 2

    def test_times_out_gracefully(self):
        handle = _make_handle()
        # Never returns Ended
        handle._request = MagicMock(return_value={"state": "Finalizing", "result": None})

        # Should not raise — just logs a warning and returns
        handle.cancel_job("job-123", poll_interval=0, max_wait=0)

        # POST + at least one timeout check
        assert handle._request.call_count >= 1


class TestCancelOrphanedJobs:
    """cancel_orphaned_jobs lists active jobs and cancels each."""

    def test_cancels_active_jobs(self):
        handle = _make_handle()
        active_jobs = [
            {"jobId": "job-1", "name": "events_daily_batch_1"},
            {"jobId": "job-2", "name": "events_daily_batch_2"},
        ]
        handle.list_jobs = MagicMock(return_value=active_jobs)
        handle.cancel_job = MagicMock()

        cancelled = handle.cancel_orphaned_jobs("events_daily")

        assert cancelled == ["job-1", "job-2"]
        handle.cancel_job.assert_has_calls([call("job-1"), call("job-2")])

    def test_uses_correct_filter(self):
        handle = _make_handle()
        handle.list_jobs = MagicMock(return_value=[])
        handle.cancel_job = MagicMock()

        handle.cancel_orphaned_jobs("my_model")

        handle.list_jobs.assert_called_once_with(
            filter_expr="startswith(name,'my_model_') and state ne 'Ended'"
        )

    def test_noop_when_no_active_jobs(self):
        handle = _make_handle()
        handle.list_jobs = MagicMock(return_value=[])
        handle.cancel_job = MagicMock()

        cancelled = handle.cancel_orphaned_jobs("events_daily")

        assert cancelled == []
        handle.cancel_job.assert_not_called()

    def test_tracks_cancelled_models(self):
        handle = _make_handle()
        handle.list_jobs = MagicMock(return_value=[])
        handle.cancel_job = MagicMock()

        handle.cancel_orphaned_jobs("events_daily")

        assert "events_daily" in ScopeConnectionHandle._cancelled_models

    def test_skips_already_cancelled_model(self):
        handle = _make_handle()
        handle.list_jobs = MagicMock(return_value=[])
        handle.cancel_job = MagicMock()

        handle.cancel_orphaned_jobs("events_daily")
        handle.list_jobs.reset_mock()

        # Second call should be a no-op
        cancelled = handle.cancel_orphaned_jobs("events_daily")

        assert cancelled == []
        handle.list_jobs.assert_not_called()

    def test_different_models_cancelled_independently(self):
        handle = _make_handle()
        handle.list_jobs = MagicMock(return_value=[])
        handle.cancel_job = MagicMock()

        handle.cancel_orphaned_jobs("model_a")
        handle.cancel_orphaned_jobs("model_b")

        assert "model_a" in ScopeConnectionHandle._cancelled_models
        assert "model_b" in ScopeConnectionHandle._cancelled_models
        assert handle.list_jobs.call_count == 2

    def test_list_failure_is_best_effort(self):
        handle = _make_handle()
        handle.list_jobs = MagicMock(side_effect=Exception("network error"))
        handle.cancel_job = MagicMock()

        # Should not raise
        cancelled = handle.cancel_orphaned_jobs("events_daily")

        assert cancelled == []
        # Model should still be marked to avoid retrying
        assert "events_daily" in ScopeConnectionHandle._cancelled_models

    def test_individual_cancel_failure_continues(self):
        handle = _make_handle()
        active_jobs = [
            {"jobId": "job-1", "name": "events_daily_batch_1"},
            {"jobId": "job-2", "name": "events_daily_batch_2"},
            {"jobId": "job-3", "name": "events_daily_batch_3"},
        ]
        handle.list_jobs = MagicMock(return_value=active_jobs)
        handle.cancel_job = MagicMock(side_effect=[None, Exception("cancel failed"), None])

        cancelled = handle.cancel_orphaned_jobs("events_daily")

        # job-1 and job-3 succeeded, job-2 failed but didn't stop the loop
        assert cancelled == ["job-1", "job-3"]
        assert handle.cancel_job.call_count == 3


# =====================================================================
# Integration: execute() triggers orphan cancellation
# =====================================================================


class TestExecuteOrphanCancellation:
    """ScopeConnectionManager.execute() cancels orphans on first call per model."""

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
        handle._next_job_au = None
        handle._next_job_priority = None
        handle._next_job_timeout_seconds = None
        handle._next_job_model_name = None
        handle.submit_and_wait = MagicMock()
        handle.submit_and_wait.return_value = MagicMock(job_id="test-id", result="Succeeded")
        handle.cancel_orphaned_jobs = MagicMock(return_value=[])

        connection = MagicMock()
        connection.handle = handle
        connection.credentials = MagicMock(
            au=100, priority=1, poll_interval_seconds=5, job_timeout_seconds=60
        )
        mgr.get_thread_connection.return_value = connection
        return mgr, handle

    def test_cancels_orphans_when_model_name_set(self, mock_manager):
        mgr, handle = mock_manager
        handle._next_job_model_name = "events_daily"

        mgr.execute(_DUMMY_SCRIPT)

        handle.cancel_orphaned_jobs.assert_called_once_with("events_daily")

    def test_no_cancellation_without_model_name(self, mock_manager):
        mgr, handle = mock_manager
        handle._next_job_model_name = None

        mgr.execute(_DUMMY_SCRIPT)

        handle.cancel_orphaned_jobs.assert_not_called()

    def test_model_name_passed_to_submit_and_wait(self, mock_manager):
        mgr, handle = mock_manager
        handle._next_job_model_name = "events_daily"

        mgr.execute(_DUMMY_SCRIPT)

        call_kwargs = handle.submit_and_wait.call_args
        assert call_kwargs.kwargs["model_name"] == "events_daily"

    def test_model_name_none_passed_when_not_set(self, mock_manager):
        mgr, handle = mock_manager
        handle._next_job_model_name = None

        mgr.execute(_DUMMY_SCRIPT)

        call_kwargs = handle.submit_and_wait.call_args
        assert call_kwargs.kwargs["model_name"] is None

    def test_model_name_persists_across_calls(self, mock_manager):
        """_next_job_model_name is NOT cleared after execute — it persists for
        subsequent batches in the same materialization."""
        mgr, handle = mock_manager
        handle._next_job_model_name = "events_daily"

        mgr.execute(_DUMMY_SCRIPT)

        # Model name should still be set
        assert handle._next_job_model_name == "events_daily"

    def test_skipped_scripts_dont_trigger_cancellation(self, mock_manager):
        mgr, handle = mock_manager
        handle._next_job_model_name = "events_daily"

        # Comment-only script is skipped
        mgr.execute("-- no-op: skipped")

        handle.cancel_orphaned_jobs.assert_not_called()
        handle.submit_and_wait.assert_not_called()
        # Model name should still be set
        assert handle._next_job_model_name == "events_daily"


class TestNextJobModelNameOnHandle:
    """_next_job_model_name initialization and behavior."""

    def test_defaults_to_none(self):
        handle = _make_handle()
        assert handle._next_job_model_name is None

    def test_can_be_set_and_read(self):
        handle = _make_handle()
        handle._next_job_model_name = "my_model"
        assert handle._next_job_model_name == "my_model"
