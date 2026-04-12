"""Unit tests for trigger lifecycle — wait_for_next_cycle, signal handling, timeouts."""

from __future__ import annotations

import threading
from unittest.mock import patch

import pytest

from dbt.adapters.scope import impl as impl_module
from dbt.adapters.scope.constants import DEFAULT_PROCESSING_TIME_TIMEOUT_SECONDS


class _AdapterStub:
    """Minimal stub with wait_for_next_cycle / reset_cycle_count bound."""

    def __init__(self) -> None:
        self._cycle_counts: dict[int, int] = {}

    wait_for_next_cycle = impl_module.ScopeAdapter.wait_for_next_cycle
    reset_cycle_count = impl_module.ScopeAdapter.reset_cycle_count


class TestWaitForNextCycle:
    """Tests for the wait_for_next_cycle logic without a full adapter instance."""

    @pytest.fixture(autouse=True)
    def _reset_shutdown(self) -> None:
        """Ensure shutdown event is clear before each test."""
        impl_module._shutdown_event.clear()

    def _make_stub(self) -> _AdapterStub:
        return _AdapterStub()

    def test_returns_true_to_continue(self) -> None:
        stub = self._make_stub()
        with patch.object(impl_module._shutdown_event, "wait", return_value=False):
            result = stub.wait_for_next_cycle(10.0, max_cycles=None)
        assert result is True

    def test_max_cycles_exceeded_returns_false(self) -> None:
        stub = self._make_stub()
        tid = threading.get_ident()
        stub._cycle_counts[tid] = 2
        with patch.object(impl_module._shutdown_event, "wait", return_value=False):
            result = stub.wait_for_next_cycle(10.0, max_cycles=3)
        assert result is False

    def test_max_cycles_not_yet_reached(self) -> None:
        stub = self._make_stub()
        tid = threading.get_ident()
        stub._cycle_counts[tid] = 1
        with patch.object(impl_module._shutdown_event, "wait", return_value=False):
            result = stub.wait_for_next_cycle(10.0, max_cycles=3)
        assert result is True

    def test_shutdown_before_sleep_returns_false(self) -> None:
        stub = self._make_stub()
        impl_module._shutdown_event.set()
        result = stub.wait_for_next_cycle(10.0, max_cycles=None)
        assert result is False

    def test_shutdown_during_sleep_returns_false(self) -> None:
        stub = self._make_stub()
        with (
            patch.object(impl_module._shutdown_event, "wait", return_value=True),
            patch.object(impl_module._shutdown_event, "is_set", return_value=False),
        ):
            result = stub.wait_for_next_cycle(10.0, max_cycles=None)
        assert result is False

    def test_cycle_count_increments(self) -> None:
        stub = self._make_stub()
        tid = threading.get_ident()
        with patch.object(impl_module._shutdown_event, "wait", return_value=False):
            stub.wait_for_next_cycle(1.0)
            assert stub._cycle_counts[tid] == 1
            stub.wait_for_next_cycle(1.0)
            assert stub._cycle_counts[tid] == 2

    def test_reset_cycle_count(self) -> None:
        stub = self._make_stub()
        tid = threading.get_ident()
        stub._cycle_counts[tid] = 5
        stub.reset_cycle_count()
        assert tid not in stub._cycle_counts

    def test_sleep_uses_interval(self) -> None:
        stub = self._make_stub()
        with patch.object(impl_module._shutdown_event, "wait", return_value=False) as mock_wait:
            stub.wait_for_next_cycle(42.5)
            mock_wait.assert_called_once_with(timeout=42.5)


class TestSignalHandlers:
    """Tests for signal handler installation."""

    @pytest.fixture(autouse=True)
    def _reset_state(self) -> None:
        impl_module._shutdown_event.clear()
        impl_module._signal_handlers_installed = False

    def test_install_is_idempotent(self) -> None:
        impl_module._install_signal_handlers()
        impl_module._install_signal_handlers()

    def test_shutdown_event_is_module_level(self) -> None:
        assert isinstance(impl_module._shutdown_event, threading.Event)
        assert not impl_module._shutdown_event.is_set()


class TestProcessingTimeTimeout:
    """Tests for auto-timeout behavior."""

    def test_default_timeout_value(self) -> None:
        assert DEFAULT_PROCESSING_TIME_TIMEOUT_SECONDS == 2_592_000  # 30 days
