"""Tests for graceful shutdown: cancel in-flight ADLA jobs on SIGINT/SIGTERM."""

from __future__ import annotations

import signal
import threading
import time
from unittest.mock import MagicMock, patch

import pytest
import requests.exceptions
from dbt_common.exceptions import DbtRuntimeError

from dbt.adapters.scope import connections as conn_module
from dbt.adapters.scope import impl as impl_module
from dbt.adapters.scope.connections import (
    ScopeConnectionHandle,
    ScopeConnectionManager,
    _active_jobs,
    _ActiveJobEntry,
    _cancelled_job_ids,
    _deregister_active_job,
    _register_active_job,
    _shutdown_event,
    cancel_all_active_jobs,
)
from dbt.adapters.scope.credentials import ScopeCredentials

# ---------------------------------------------------------------------------
# Test fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def _reset_shutdown_state():
    """Snapshot/restore module-level state mutated by these tests."""
    _shutdown_event.clear()
    saved_jobs = dict(_active_jobs)
    saved_cancelled = set(_cancelled_job_ids)
    saved_observed = list(impl_module._observed_credentials)
    _active_jobs.clear()
    _cancelled_job_ids.clear()
    impl_module._observed_credentials.clear()
    yield
    _shutdown_event.clear()
    _active_jobs.clear()
    _active_jobs.update(saved_jobs)
    _cancelled_job_ids.clear()
    _cancelled_job_ids.update(saved_cancelled)
    impl_module._observed_credentials.clear()
    impl_module._observed_credentials.extend(saved_observed)


def _make_handle(account: str = "test-adla") -> ScopeConnectionHandle:
    creds = MagicMock()
    creds.adla_account = account
    creds.http_timeout_seconds = 30
    creds.http_retries = 3
    with patch.object(ScopeConnectionHandle, "_build_session", return_value=MagicMock()):
        return ScopeConnectionHandle(creds)


def _make_entry(job_id: str, handle: ScopeConnectionHandle | None = None) -> _ActiveJobEntry:
    return _ActiveJobEntry(
        job_id=job_id,
        name=f"job-{job_id}",
        handle=handle or _make_handle(),
        submitted_at=time.monotonic(),
        model_name=None,
    )


# ---------------------------------------------------------------------------
# Registry primitives
# ---------------------------------------------------------------------------


class TestActiveJobRegistry:
    def test_register_and_deregister(self):
        entry = _make_entry("job-1")
        _register_active_job(entry)
        assert "job-1" in _active_jobs
        _deregister_active_job("job-1")
        assert "job-1" not in _active_jobs

    def test_deregister_unknown_is_noop(self):
        _deregister_active_job("does-not-exist")  # must not raise


# ---------------------------------------------------------------------------
# submit_and_wait: register/deregister + shutdown abort
# ---------------------------------------------------------------------------


class TestSubmitAndWaitRegistry:
    def _handle_with_request(self, fake_request):
        handle = _make_handle()
        handle._get_token = MagicMock(return_value="fake-token")
        handle._request = MagicMock(side_effect=fake_request)
        return handle

    def test_registers_and_deregisters_on_success(self):
        seen_during_poll: list[str] = []

        def fake_request(method, url, **kwargs):
            if method == "PUT":
                return {"state": "Running"}
            seen_during_poll.extend(_active_jobs.keys())
            return {"state": "Ended", "result": "Succeeded"}

        handle = self._handle_with_request(fake_request)
        job = handle.submit_and_wait(name="t", script="// s", au=10, priority=1, poll_interval=0)
        assert job.succeeded
        assert seen_during_poll == [job.job_id]
        assert job.job_id not in _active_jobs

    def test_deregisters_on_failure(self):
        def fake_request(method, url, **kwargs):
            if method == "PUT":
                return {"state": "Running"}
            return {"state": "Ended", "result": "Failed", "errorMessage": "boom"}

        handle = self._handle_with_request(fake_request)
        from dbt_common.exceptions import DbtDatabaseError

        with pytest.raises(DbtDatabaseError):
            handle.submit_and_wait(name="t", script="// s", au=10, priority=1, poll_interval=0)
        assert _active_jobs == {}

    def test_aborts_on_shutdown_event_and_self_cancels(self):
        cancel_calls: list[tuple[str, int]] = []

        def fake_cancel_job(job_id, poll_interval=2, max_wait=120):
            cancel_calls.append((job_id, max_wait))

        def fake_request(method, url, **kwargs):
            if method == "PUT":
                return {"state": "Running"}
            return {"state": "Running"}

        handle = self._handle_with_request(fake_request)
        handle.cancel_job = MagicMock(side_effect=fake_cancel_job)
        _shutdown_event.set()

        with pytest.raises(DbtRuntimeError, match="shutdown signal"):
            handle.submit_and_wait(
                name="t",
                script="// s",
                au=10,
                priority=1,
                poll_interval=0,
                wait_on_cancel_seconds=17,
            )
        assert len(cancel_calls) == 1
        assert cancel_calls[0][1] == 17
        assert _active_jobs == {}
        assert cancel_calls[0][0] in _cancelled_job_ids

    def test_does_not_double_cancel_if_already_in_cancelled_set(self):
        cancel_mock = MagicMock()

        def fake_request(method, url, **kwargs):
            if method == "PUT":
                return {"jobId": "preset-id", "state": "Running"}
            return {"state": "Running"}

        handle = self._handle_with_request(fake_request)
        handle.cancel_job = cancel_mock

        with patch("uuid.uuid4", return_value=MagicMock(__str__=lambda self: "preset-id")):
            _cancelled_job_ids.add("preset-id")
            _shutdown_event.set()
            with pytest.raises(DbtRuntimeError):
                handle.submit_and_wait(name="t", script="// s", au=10, priority=1, poll_interval=0)
        cancel_mock.assert_not_called()


# ---------------------------------------------------------------------------
# cancel_all_active_jobs
# ---------------------------------------------------------------------------


class TestCancelAllActiveJobs:
    def test_empty_registry_returns_zero(self):
        assert cancel_all_active_jobs("test", wait_seconds=1) == (0, 0)

    def test_calls_cancel_job_per_entry_with_wait(self):
        handle_a = _make_handle("a")
        handle_b = _make_handle("b")
        handle_a.cancel_job = MagicMock()
        handle_b.cancel_job = MagicMock()
        _register_active_job(_make_entry("job-a", handle_a))
        _register_active_job(_make_entry("job-b", handle_b))

        attempted, confirmed = cancel_all_active_jobs("test", wait_seconds=11)

        assert attempted == 2
        assert confirmed == 2
        handle_a.cancel_job.assert_called_once_with("job-a", poll_interval=2, max_wait=11)
        handle_b.cancel_job.assert_called_once_with("job-b", poll_interval=2, max_wait=11)

    def test_returns_attempted_and_confirmed_with_mixed_results(self):
        good = _make_handle("good")
        bad = _make_handle("bad")
        good.cancel_job = MagicMock()
        bad.cancel_job = MagicMock(side_effect=RuntimeError("network down"))
        _register_active_job(_make_entry("ok", good))
        _register_active_job(_make_entry("err", bad))

        attempted, confirmed = cancel_all_active_jobs("test", wait_seconds=5)

        assert attempted == 2
        assert confirmed == 1

    def test_continues_on_per_job_failure(self):
        h1 = _make_handle("h1")
        h2 = _make_handle("h2")
        h3 = _make_handle("h3")
        h1.cancel_job = MagicMock()
        h2.cancel_job = MagicMock(side_effect=ValueError("boom"))
        h3.cancel_job = MagicMock()
        _register_active_job(_make_entry("j1", h1))
        _register_active_job(_make_entry("j2", h2))
        _register_active_job(_make_entry("j3", h3))

        cancel_all_active_jobs("test", wait_seconds=5)

        h1.cancel_job.assert_called_once()
        h2.cancel_job.assert_called_once()
        h3.cancel_job.assert_called_once()

    def test_respects_wait_ceiling(self):
        slow = _make_handle("slow")

        def slow_cancel(job_id, poll_interval=2, max_wait=120):
            time.sleep(max_wait + 10)

        slow.cancel_job = MagicMock(side_effect=slow_cancel)
        _register_active_job(_make_entry("slow-job", slow))

        start = time.monotonic()
        cancel_all_active_jobs("test", wait_seconds=1)
        elapsed = time.monotonic() - start

        # wait_seconds=1 + 5s grace = 6s ceiling
        assert elapsed < 8, f"Cancel-all blocked {elapsed:.1f}s, expected < 8s"

    def test_skips_jobs_already_in_cancelled_set(self):
        handle = _make_handle()
        handle.cancel_job = MagicMock()
        _register_active_job(_make_entry("already-cancelled", handle))
        _cancelled_job_ids.add("already-cancelled")

        attempted, confirmed = cancel_all_active_jobs("test", wait_seconds=5)

        assert attempted == 1
        assert confirmed == 1
        handle.cancel_job.assert_not_called()


# ---------------------------------------------------------------------------
# Observed credentials gates in impl.py
# ---------------------------------------------------------------------------


class TestObservedCredentialsGates:
    def _make_creds(self, *, cancel=True, wait=30):
        creds = ScopeCredentials(
            database="db",
            schema="sch",
            adla_account="acct",
            cancel_jobs_on_shutdown=cancel,
            wait_on_cancel_seconds=wait,
        )
        return creds

    def test_no_observed_defaults_to_enabled(self):
        assert impl_module._any_observed_cancel_on_shutdown_enabled() is True
        assert impl_module._observed_max_wait_on_cancel_seconds() == 30

    def test_all_opt_out(self):
        impl_module._observe_credentials(self._make_creds(cancel=False))
        impl_module._observe_credentials(self._make_creds(cancel=False))
        assert impl_module._any_observed_cancel_on_shutdown_enabled() is False

    def test_any_enabled_triggers_gate(self):
        impl_module._observe_credentials(self._make_creds(cancel=False))
        impl_module._observe_credentials(self._make_creds(cancel=True, wait=42))
        assert impl_module._any_observed_cancel_on_shutdown_enabled() is True

    def test_observed_max_wait_returns_max_across_credentials(self):
        impl_module._observe_credentials(self._make_creds(wait=30))
        impl_module._observe_credentials(self._make_creds(wait=60))
        impl_module._observe_credentials(self._make_creds(wait=10))
        assert impl_module._observed_max_wait_on_cancel_seconds() == 60

    def test_observed_max_wait_ignores_opt_out_entries(self):
        impl_module._observe_credentials(self._make_creds(cancel=False, wait=999))
        impl_module._observe_credentials(self._make_creds(cancel=True, wait=20))
        assert impl_module._observed_max_wait_on_cancel_seconds() == 20

    def test_observe_credentials_is_idempotent(self):
        c = self._make_creds()
        impl_module._observe_credentials(c)
        impl_module._observe_credentials(c)
        assert len(impl_module._observed_credentials) == 1


# ---------------------------------------------------------------------------
# Signal handler + atexit
# ---------------------------------------------------------------------------


class TestSignalHandlerCancelAll:
    def test_signal_handler_invokes_cancel_all(self):
        impl_module._observe_credentials(
            ScopeCredentials(
                database="db",
                schema="sch",
                adla_account="acct",
                wait_on_cancel_seconds=42,
            )
        )
        with (
            patch.object(impl_module, "cancel_all_active_jobs") as mock_cancel,
            patch.object(impl_module, "_signal_handlers_installed", False),
        ):
            old_sigint = signal.getsignal(signal.SIGINT)
            old_sigterm = signal.getsignal(signal.SIGTERM)
            # Set SIGINT to SIG_IGN so the chained previous handler doesn't
            # raise KeyboardInterrupt inside the test.
            signal.signal(signal.SIGINT, signal.SIG_IGN)
            signal.signal(signal.SIGTERM, signal.SIG_IGN)
            try:
                impl_module._install_signal_handlers()
                handler = signal.getsignal(signal.SIGINT)
                assert callable(handler)
                handler(signal.SIGINT, None)
            finally:
                signal.signal(signal.SIGINT, old_sigint)
                signal.signal(signal.SIGTERM, old_sigterm)
                impl_module._signal_handlers_installed = False
        mock_cancel.assert_called_once()
        args, kwargs = mock_cancel.call_args
        assert args[0] == "signal:SIGINT"
        assert kwargs.get("wait_seconds") == 42
        assert _shutdown_event.is_set()

    def test_signal_handler_skipped_when_all_opted_out(self):
        impl_module._observe_credentials(
            ScopeCredentials(
                database="db",
                schema="sch",
                adla_account="acct",
                cancel_jobs_on_shutdown=False,
            )
        )
        with (
            patch.object(impl_module, "cancel_all_active_jobs") as mock_cancel,
            patch.object(impl_module, "_signal_handlers_installed", False),
        ):
            old_sigint = signal.getsignal(signal.SIGINT)
            old_sigterm = signal.getsignal(signal.SIGTERM)
            signal.signal(signal.SIGINT, signal.SIG_IGN)
            signal.signal(signal.SIGTERM, signal.SIG_IGN)
            try:
                impl_module._install_signal_handlers()
                handler = signal.getsignal(signal.SIGINT)
                handler(signal.SIGINT, None)
            finally:
                signal.signal(signal.SIGINT, old_sigint)
                signal.signal(signal.SIGTERM, old_sigterm)
                impl_module._signal_handlers_installed = False
        mock_cancel.assert_not_called()
        # _shutdown_event is still set so in-flight loops abort
        assert _shutdown_event.is_set()


class TestAtexitCancelAll:
    def test_atexit_invokes_cancel_all_when_enabled(self):
        impl_module._observe_credentials(
            ScopeCredentials(
                database="db",
                schema="sch",
                adla_account="acct",
                wait_on_cancel_seconds=15,
            )
        )
        with patch.object(impl_module, "cancel_all_active_jobs") as mock_cancel:
            impl_module._atexit_cancel_all()
        mock_cancel.assert_called_once()
        args, kwargs = mock_cancel.call_args
        assert args[0] == "atexit"
        assert kwargs.get("wait_seconds") == 15

    def test_atexit_skipped_when_all_opted_out(self):
        impl_module._observe_credentials(
            ScopeCredentials(
                database="db",
                schema="sch",
                adla_account="acct",
                cancel_jobs_on_shutdown=False,
            )
        )
        with patch.object(impl_module, "cancel_all_active_jobs") as mock_cancel:
            impl_module._atexit_cancel_all()
        mock_cancel.assert_not_called()


# ---------------------------------------------------------------------------
# dbt-native cancel hooks
# ---------------------------------------------------------------------------


class TestIsCancelable:
    def test_returns_true(self):
        assert impl_module.ScopeAdapter.is_cancelable() is True


class TestManagerCancelDelegation:
    def test_cancel_delegates_to_cancel_all(self):
        connection = MagicMock()
        connection.credentials = ScopeCredentials(
            database="db",
            schema="sch",
            adla_account="acct",
            wait_on_cancel_seconds=21,
        )
        mgr = ScopeConnectionManager.__new__(ScopeConnectionManager)
        with patch.object(conn_module, "cancel_all_active_jobs") as mock_cancel:
            mgr.cancel(connection)
        mock_cancel.assert_called_once_with("dbt-native:cancel", wait_seconds=21)

    def test_cancel_respects_opt_out_credential(self):
        connection = MagicMock()
        connection.credentials = ScopeCredentials(
            database="db",
            schema="sch",
            adla_account="acct",
            cancel_jobs_on_shutdown=False,
        )
        mgr = ScopeConnectionManager.__new__(ScopeConnectionManager)
        with patch.object(conn_module, "cancel_all_active_jobs") as mock_cancel:
            mgr.cancel(connection)
        mock_cancel.assert_not_called()

    def test_cancel_open_delegates_to_cancel_all(self):
        with patch.object(conn_module, "cancel_all_active_jobs") as mock_cancel:
            ScopeConnectionManager.cancel_open()
        mock_cancel.assert_called_once_with("dbt-native:cancel_open", wait_seconds=30)


# ---------------------------------------------------------------------------
# Connection open hook wires everything
# ---------------------------------------------------------------------------


class TestOpenHookInstallation:
    def test_open_hook_is_wired(self):
        assert ScopeConnectionManager._on_open is not None
        # impl.py sets it to _scope_open_hook
        assert ScopeConnectionManager._on_open is impl_module._scope_open_hook

    def test_open_hook_observes_credentials_and_installs_handlers(self):
        creds = ScopeCredentials(
            database="db",
            schema="sch",
            adla_account="acct",
            wait_on_cancel_seconds=55,
        )
        with (
            patch.object(impl_module, "_install_signal_handlers") as install_mock,
            patch.object(impl_module, "_register_atexit") as atexit_mock,
        ):
            impl_module._scope_open_hook(creds)
        assert creds in impl_module._observed_credentials
        install_mock.assert_called_once()
        atexit_mock.assert_called_once()


# ---------------------------------------------------------------------------
# Sanity: poll loop tolerates exceptions raised by cancel_job during shutdown
# ---------------------------------------------------------------------------


class TestSelfCancelFailureDuringShutdown:
    def test_self_cancel_exception_still_raises_runtime_error(self):
        handle = _make_handle()
        handle._get_token = MagicMock(return_value="fake-token")

        def fake_request(method, url, **kwargs):
            if method == "PUT":
                return {"state": "Running"}
            return {"state": "Running"}

        handle._request = MagicMock(side_effect=fake_request)
        handle.cancel_job = MagicMock(side_effect=requests.exceptions.ConnectionError("nope"))
        _shutdown_event.set()

        with pytest.raises(DbtRuntimeError, match="shutdown signal"):
            handle.submit_and_wait(
                name="t",
                script="// s",
                au=10,
                priority=1,
                poll_interval=0,
                wait_on_cancel_seconds=3,
            )
        # Still deregistered
        assert _active_jobs == {}


# ---------------------------------------------------------------------------
# Thread-safety smoke
# ---------------------------------------------------------------------------


class TestRegistryThreadSafety:
    def test_concurrent_register_deregister(self):
        def worker(i: int):
            for j in range(50):
                e = _make_entry(f"j-{i}-{j}")
                _register_active_job(e)
                _deregister_active_job(e.job_id)

        threads = [threading.Thread(target=worker, args=(i,)) for i in range(8)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        assert _active_jobs == {}
