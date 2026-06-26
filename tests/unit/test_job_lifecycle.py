"""Tests for ADLA job lifecycle: related metadata, list_jobs, and orphan cancellation."""

from __future__ import annotations

import uuid
from contextlib import contextmanager
from unittest.mock import MagicMock, call, patch
from urllib.parse import quote as url_quote

import pytest
import requests.exceptions
from dbt_common.exceptions import DbtDatabaseError

from dbt.adapters.scope.connections import (
    _UUID_NAMESPACE,
    ScopeConnectionHandle,
    ScopeConnectionManager,
)
from dbt.adapters.scope.message_retry import MessageRetryPolicy

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

    def test_skips_jobs_from_current_run(self):
        """Jobs with the current _run_id are siblings, not orphans — skip them."""
        handle = _make_handle()
        current_run = ScopeConnectionHandle._run_id
        active_jobs = [
            # Orphan from a previous run
            {
                "jobId": "job-old",
                "name": "events_daily_batch_1",
                "related": {"runId": "previous-run-id"},
            },
            # Sibling from the current run — must NOT be cancelled
            {
                "jobId": "job-sibling",
                "name": "events_daily_batch_2",
                "related": {"runId": current_run},
            },
        ]
        handle.list_jobs = MagicMock(return_value=active_jobs)
        handle.cancel_job = MagicMock()

        cancelled = handle.cancel_orphaned_jobs("events_daily")

        assert cancelled == ["job-old"]
        handle.cancel_job.assert_called_once_with("job-old")

    def test_cancels_jobs_without_related_metadata(self):
        """Jobs without related metadata are treated as orphans (legacy jobs)."""
        handle = _make_handle()
        active_jobs = [
            {"jobId": "job-legacy", "name": "events_daily_batch_1"},
        ]
        handle.list_jobs = MagicMock(return_value=active_jobs)
        handle.cancel_job = MagicMock()

        cancelled = handle.cancel_orphaned_jobs("events_daily")

        assert cancelled == ["job-legacy"]


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
        handle._job_retry_policy = MessageRetryPolicy.disabled()
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


_VERTEX_TIMEOUT_FAILURE = (
    "SCOPE job 'm_incremental_batch_1_of_1_files_6' (5838e41a) failed: "
    "Exception in VertexManager, vertex:vertex_1 [SV16_Process],"
    "GrapheneJniException: Failed to open stream dms://x/y_0_0 with error Operation timed out"
)
_FATAL_FAILURE = "SCOPE job 'm' (abc) failed: E_USER_ERROR: syntax error near 'SELECT'"


class TestExecuteJobRetry:
    """execute() re-submits a job on a transient regex-matched failure (#39/#40)."""

    def _manager(self, submit_side_effect):
        from dbt.adapters.scope.message_retry import MessageRetryPolicy

        mgr = MagicMock(spec=ScopeConnectionManager)
        mgr.execute = ScopeConnectionManager.execute.__get__(mgr, ScopeConnectionManager)

        @contextmanager
        def _exception_handler(sql):
            try:
                yield
            except Exception as exc:
                if isinstance(exc, DbtDatabaseError):
                    raise
                raise DbtDatabaseError(str(exc)) from exc

        mgr.exception_handler = _exception_handler

        handle = MagicMock()
        for attr in (
            "_next_job_name",
            "_next_job_au",
            "_next_job_priority",
            "_next_job_timeout_seconds",
            "_next_job_model_name",
        ):
            setattr(handle, attr, None)

        handle._job_retry_policy = MessageRetryPolicy.for_job_retry(
            MagicMock(
                enable_job_retry=True,
                job_retry_on_messages=[],
                job_retry_max_attempts=3,
                job_retry_initial_wait_seconds=0.01,
                job_retry_max_wait_seconds=0.01,
            )
        )
        handle.submit_and_wait = MagicMock(side_effect=submit_side_effect)
        handle.cancel_orphaned_jobs = MagicMock(return_value=[])

        connection = MagicMock()
        connection.handle = handle
        connection.credentials = MagicMock(
            au=100, priority=1, poll_interval_seconds=5, job_timeout_seconds=60
        )
        mgr.get_thread_connection.return_value = connection
        return mgr, handle

    def test_resubmits_on_transient_failure_then_succeeds(self):
        ok = MagicMock(job_id="job-2", result="Succeeded")
        mgr, handle = self._manager(
            submit_side_effect=[DbtDatabaseError(_VERTEX_TIMEOUT_FAILURE), ok]
        )
        with patch("dbt.adapters.scope.message_retry.time.sleep"):
            resp, _ = mgr.execute(_DUMMY_SCRIPT)
        assert handle.submit_and_wait.call_count == 2
        assert "job-2" in resp._message

    def test_does_not_retry_fatal_failure(self):
        mgr, handle = self._manager(submit_side_effect=DbtDatabaseError(_FATAL_FAILURE))
        with pytest.raises(DbtDatabaseError, match="E_USER_ERROR"):
            mgr.execute(_DUMMY_SCRIPT)
        assert handle.submit_and_wait.call_count == 1

    def test_orphan_cancel_runs_once_despite_retries(self):
        ok = MagicMock(job_id="job-2", result="Succeeded")
        mgr, handle = self._manager(
            submit_side_effect=[DbtDatabaseError(_VERTEX_TIMEOUT_FAILURE), ok]
        )
        handle._next_job_model_name = "events_daily"
        with patch("dbt.adapters.scope.message_retry.time.sleep"):
            mgr.execute(_DUMMY_SCRIPT)
        handle.cancel_orphaned_jobs.assert_called_once_with("events_daily")
        assert handle.submit_and_wait.call_count == 2


# =====================================================================
# Part D: Transient poll error resilience in submit_and_wait
# =====================================================================


class TestSubmitAndWaitTransientErrors:
    """submit_and_wait tolerates transient poll failures up to a threshold."""

    def _make_handle_with_submit(self):
        handle = _make_handle()
        handle._get_token = MagicMock(return_value="fake-token")
        handle._request = MagicMock(return_value={"state": "Preparing"})
        return handle

    def test_recovers_from_transient_timeout(self):
        """A single ReadTimeout during polling should not crash the run."""
        handle = self._make_handle_with_submit()
        call_count = 0

        def fake_request(method, url, **kwargs):
            nonlocal call_count
            call_count += 1
            if method == "PUT":
                return {"state": "Running"}
            # First poll: timeout, second poll: success
            if call_count == 3:
                raise requests.exceptions.ReadTimeout("Read timed out")
            return {"state": "Ended", "result": "Succeeded"}

        handle._request = MagicMock(side_effect=fake_request)

        job = handle.submit_and_wait(
            name="test-job", script="// script", au=10, priority=1, poll_interval=0
        )
        assert job.succeeded

    def test_recovers_from_transient_connection_error(self):
        """A ConnectionError during polling should not crash the run."""
        handle = self._make_handle_with_submit()
        call_count = 0

        def fake_request(method, url, **kwargs):
            nonlocal call_count
            call_count += 1
            if method == "PUT":
                return {"state": "Running"}
            if call_count == 3:
                raise requests.exceptions.ConnectionError("Connection reset")
            return {"state": "Ended", "result": "Succeeded"}

        handle._request = MagicMock(side_effect=fake_request)

        job = handle.submit_and_wait(
            name="test-job", script="// script", au=10, priority=1, poll_interval=0
        )
        assert job.succeeded

    def test_consecutive_failures_exceed_threshold_raises(self):
        """Exceeding _MAX_CONSECUTIVE_POLL_FAILURES consecutive errors raises."""
        handle = self._make_handle_with_submit()
        call_count = 0

        def fake_request(method, url, **kwargs):
            nonlocal call_count
            call_count += 1
            if method == "PUT":
                return {"state": "Running"}
            raise requests.exceptions.ReadTimeout("Read timed out")

        handle._request = MagicMock(side_effect=fake_request)

        from dbt_common.exceptions import DbtDatabaseError

        with pytest.raises(DbtDatabaseError, match="poll failed"):
            handle.submit_and_wait(
                name="test-job", script="// script", au=10, priority=1, poll_interval=0
            )

    def test_counter_resets_on_successful_poll(self):
        """Consecutive failure counter resets after a successful poll."""
        handle = self._make_handle_with_submit()
        call_count = 0

        def fake_request(method, url, **kwargs):
            nonlocal call_count
            call_count += 1
            if method == "PUT":
                return {"state": "Running"}
            # Pattern: 4 failures, 1 success, 4 failures, 1 success (terminal)
            poll_num = call_count - 1  # subtract the PUT
            if poll_num <= 4:
                raise requests.exceptions.ReadTimeout("Read timed out")
            if poll_num == 5:
                return {"state": "Running"}
            if poll_num <= 9:
                raise requests.exceptions.ReadTimeout("Read timed out")
            return {"state": "Ended", "result": "Succeeded"}

        handle._request = MagicMock(side_effect=fake_request)

        # Should succeed — never hits 5 consecutive failures
        job = handle.submit_and_wait(
            name="test-job", script="// script", au=10, priority=1, poll_interval=0
        )
        assert job.succeeded
