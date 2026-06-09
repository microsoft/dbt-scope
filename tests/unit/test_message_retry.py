"""Tests for MessageRetryPolicy + retry_on_message."""

from __future__ import annotations

import re

import pytest
from dbt_common.exceptions import DbtDatabaseError

from dbt.adapters.scope.message_retry import (
    MessageRetryPolicy,
    retry_on_message,
)


class _Creds:
    """Tiny duck-typed stand-in for ScopeCredentials."""

    def __init__(
        self,
        retry_on_error_messages=None,
        max_retries_on_error=25,
        initial_wait_on_error_seconds=1.0,
        max_wait_on_error_seconds=30.0,
    ):
        self.retry_on_error_messages = retry_on_error_messages
        self.max_retries_on_error = max_retries_on_error
        self.initial_wait_on_error_seconds = initial_wait_on_error_seconds
        self.max_wait_on_error_seconds = max_wait_on_error_seconds


class TestMessageRetryPolicyConstruction:
    def test_disabled_factory_produces_empty_policy(self):
        policy = MessageRetryPolicy.disabled()
        assert policy.patterns == ()
        assert policy.enabled is False

    def test_from_credentials_compiles_substring_and_regex(self):
        creds = _Creds(
            retry_on_error_messages=[
                "Cannot exceed",
                "re:queued \\d+ jobs",
            ],
            max_retries_on_error=3,
            initial_wait_on_error_seconds=0.5,
            max_wait_on_error_seconds=4.0,
        )

        policy = MessageRetryPolicy.from_credentials(creds)

        assert policy.max_retries == 3
        assert policy.initial_wait_seconds == 0.5
        assert policy.max_wait_seconds == 4.0
        assert len(policy.patterns) == 2
        assert policy.patterns[0] == "Cannot exceed"
        assert isinstance(policy.patterns[1], re.Pattern)
        assert policy.patterns[1].pattern == "queued \\d+ jobs"

    def test_from_credentials_with_no_patterns_disables_policy(self):
        policy = MessageRetryPolicy.from_credentials(_Creds(retry_on_error_messages=[]))
        assert policy.enabled is False

    def test_from_credentials_rejects_empty_string_entry(self):
        with pytest.raises(ValueError, match="non-empty strings"):
            MessageRetryPolicy.from_credentials(_Creds(retry_on_error_messages=[""]))

    def test_from_credentials_rejects_empty_regex_after_prefix(self):
        with pytest.raises(ValueError, match="empty after"):
            MessageRetryPolicy.from_credentials(_Creds(retry_on_error_messages=["re:"]))

    def test_from_credentials_rejects_negative_max_retries(self):
        creds = _Creds(retry_on_error_messages=["x"], max_retries_on_error=-1)
        with pytest.raises(ValueError, match="max_retries_on_error must be >= 0"):
            MessageRetryPolicy.from_credentials(creds)

    def test_from_credentials_rejects_non_positive_initial_wait(self):
        creds = _Creds(retry_on_error_messages=["x"], initial_wait_on_error_seconds=0)
        with pytest.raises(ValueError, match="initial_wait_on_error_seconds must be > 0"):
            MessageRetryPolicy.from_credentials(creds)

    def test_from_credentials_rejects_initial_greater_than_max(self):
        creds = _Creds(
            retry_on_error_messages=["x"],
            initial_wait_on_error_seconds=10,
            max_wait_on_error_seconds=5,
        )
        with pytest.raises(ValueError, match="must be <= max_wait_on_error_seconds"):
            MessageRetryPolicy.from_credentials(creds)


class TestMessageRetryPolicyMatching:
    def test_substring_match(self):
        policy = MessageRetryPolicy(patterns=("Cannot exceed",))
        assert policy.matches(RuntimeError("400: Cannot exceed 1000 queued jobs")) == (
            "Cannot exceed"
        )

    def test_regex_match_returns_pattern_label(self):
        policy = MessageRetryPolicy(patterns=(re.compile(r"Cannot exceed \d+"),))
        assert policy.matches(RuntimeError("Cannot exceed 1000 queued")) == (
            "re:Cannot exceed \\d+"
        )

    def test_no_match_returns_none(self):
        policy = MessageRetryPolicy(patterns=("Cannot exceed",))
        assert policy.matches(RuntimeError("Permission denied")) is None

    def test_empty_patterns_returns_none(self):
        policy = MessageRetryPolicy(patterns=())
        assert policy.matches(RuntimeError("Cannot exceed")) is None


class TestDelayCurve:
    def test_exponential_capped_curve(self):
        policy = MessageRetryPolicy(
            patterns=("x",),
            max_retries=10,
            initial_wait_seconds=1.0,
            max_wait_seconds=30.0,
        )
        delays = [policy.delay_for_attempt(n) for n in range(1, 11)]
        assert delays == [1.0, 2.0, 4.0, 8.0, 16.0, 30.0, 30.0, 30.0, 30.0, 30.0]


class TestRetryOnMessage:
    def _policy(self, max_retries: int = 3) -> MessageRetryPolicy:
        return MessageRetryPolicy(
            patterns=("Cannot exceed",),
            max_retries=max_retries,
            initial_wait_seconds=1.0,
            max_wait_seconds=30.0,
        )

    def test_returns_immediately_on_success(self):
        sleeps: list[float] = []
        result = retry_on_message(
            lambda: 42,
            policy=self._policy(),
            label="test",
            sleep=sleeps.append,
        )
        assert result == 42
        assert sleeps == []

    def test_disabled_policy_runs_operation_once_without_retry(self):
        calls = {"n": 0}

        def op():
            calls["n"] += 1
            raise RuntimeError("Cannot exceed 1000 queued")

        sleeps: list[float] = []
        with pytest.raises(RuntimeError, match="Cannot exceed"):
            retry_on_message(
                op,
                policy=MessageRetryPolicy.disabled(),
                label="test",
                sleep=sleeps.append,
            )
        assert calls["n"] == 1
        assert sleeps == []

    def test_retries_with_capped_exponential_backoff(self):
        attempts: list[int] = []

        def op():
            attempts.append(len(attempts) + 1)
            if len(attempts) < 7:
                raise DbtDatabaseError("ADLA 400: Cannot exceed 1000 queued SCOPE jobs")
            return "ok"

        sleeps: list[float] = []
        result = retry_on_message(
            op,
            policy=MessageRetryPolicy(
                patterns=("Cannot exceed",),
                max_retries=10,
                initial_wait_seconds=1.0,
                max_wait_seconds=30.0,
            ),
            label="test",
            sleep=sleeps.append,
        )
        assert result == "ok"
        assert len(attempts) == 7
        # 6 failures → 6 sleeps before the 7th attempt succeeds
        assert sleeps == [1.0, 2.0, 4.0, 8.0, 16.0, 30.0]

    def test_exhausts_budget_and_reraises_original_exception_type(self):
        def op():
            raise DbtDatabaseError("ADLA 400: Cannot exceed 1000 queued SCOPE jobs")

        sleeps: list[float] = []
        with pytest.raises(DbtDatabaseError, match="Cannot exceed"):
            retry_on_message(
                op,
                policy=self._policy(max_retries=3),
                label="test",
                sleep=sleeps.append,
            )
        # 4 total attempts (1 + 3 retries) → 3 sleeps
        assert sleeps == [1.0, 2.0, 4.0]

    def test_non_matching_exception_propagates_immediately(self):
        calls = {"n": 0}

        def op():
            calls["n"] += 1
            raise PermissionError("403 Forbidden — credential lacks role")

        sleeps: list[float] = []
        with pytest.raises(PermissionError):
            retry_on_message(
                op,
                policy=self._policy(),
                label="test",
                sleep=sleeps.append,
            )
        assert calls["n"] == 1
        assert sleeps == []

    def test_regex_pattern_triggers_retry(self):
        attempts = {"n": 0}

        def op():
            attempts["n"] += 1
            if attempts["n"] < 2:
                raise RuntimeError("Cannot exceed 1234 queued SCOPE jobs")
            return "ok"

        sleeps: list[float] = []
        policy = MessageRetryPolicy(
            patterns=(re.compile(r"Cannot exceed \d+ queued"),),
            max_retries=3,
            initial_wait_seconds=1.0,
            max_wait_seconds=30.0,
        )
        result = retry_on_message(op, policy=policy, label="test", sleep=sleeps.append)
        assert result == "ok"
        assert attempts["n"] == 2
        assert sleeps == [1.0]

    def test_zero_max_retries_with_patterns_still_runs_once_and_raises(self):
        calls = {"n": 0}

        def op():
            calls["n"] += 1
            raise RuntimeError("Cannot exceed 1000 queued")

        sleeps: list[float] = []
        policy = MessageRetryPolicy(
            patterns=("Cannot exceed",),
            max_retries=0,
            initial_wait_seconds=1.0,
            max_wait_seconds=30.0,
        )
        with pytest.raises(RuntimeError):
            retry_on_message(op, policy=policy, label="test", sleep=sleeps.append)
        assert calls["n"] == 1
        assert sleeps == []
