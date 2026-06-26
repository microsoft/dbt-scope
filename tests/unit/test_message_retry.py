"""Tests for MessageRetryPolicy + retry_on_message."""

from __future__ import annotations

import re

import pytest
from dbt_common.exceptions import DbtDatabaseError

from dbt.adapters.scope.message_retry import (
    DEFAULT_JOB_RETRY_RULES,
    MessageRetryPolicy,
    RetryRule,
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


_VERTEX_TIMEOUT_MSG = (
    "SCOPE job 'm_incremental_batch_1_of_1_files_6' (5838e41a) failed: "
    "Exception in VertexManager, vertex:vertex_1782102387750_7438_1_16 [SV16_Process],"
    "com.microsoft.scopeam.store.cosmos.GrapheneJniException: Failed to open stream "
    "dms://Scopeonbbc-prod/~vm-96533376/2107c3fc_0_0 with error Operation timed out"
)
_CANCELLED_MSG = (
    "SCOPE job 'mon_hue_hue_162bf0_incremental_batch_1_of_1_files_3' (bc09a681) failed: "
    "Job cancelled by user someSpn-app@SPI through ADL FE"
)
_BENIGN_MSG = "SCOPE job 'm' (abc) failed: E_USER_ERROR: syntax error near 'SELECT'"


class TestDefaultJobRetryRules:
    def test_rules_are_retry_rule_instances(self):
        assert DEFAULT_JOB_RETRY_RULES
        assert all(isinstance(r, RetryRule) for r in DEFAULT_JOB_RETRY_RULES)

    def test_rule_names_unique(self):
        names = [r.name for r in DEFAULT_JOB_RETRY_RULES]
        assert len(names) == len(set(names))

    def test_every_rule_pattern_compiles(self):
        for rule in DEFAULT_JOB_RETRY_RULES:
            re.compile(rule.pattern)

    def test_seed_rules_present(self):
        names = {r.name for r in DEFAULT_JOB_RETRY_RULES}
        assert "vertex_stream_open_timeout" in names
        assert "job_cancelled_by_user" in names


class _JobCreds:
    """Duck-typed stand-in for the job-retry slice of ScopeCredentials."""

    def __init__(
        self,
        enable_job_retry=True,
        job_retry_on_messages=None,
        job_retry_max_attempts=3,
        job_retry_initial_wait_seconds=30.0,
        job_retry_max_wait_seconds=300.0,
    ):
        self.enable_job_retry = enable_job_retry
        self.job_retry_on_messages = job_retry_on_messages
        self.job_retry_max_attempts = job_retry_max_attempts
        self.job_retry_initial_wait_seconds = job_retry_initial_wait_seconds
        self.job_retry_max_wait_seconds = job_retry_max_wait_seconds


class TestForJobRetry:
    def test_builtin_rules_match_known_failures(self):
        policy = MessageRetryPolicy.for_job_retry(_JobCreds())
        assert policy.enabled
        assert policy.matches(DbtDatabaseError(_VERTEX_TIMEOUT_MSG)) is not None
        assert policy.matches(DbtDatabaseError(_CANCELLED_MSG)) is not None

    def test_benign_failure_not_matched(self):
        policy = MessageRetryPolicy.for_job_retry(_JobCreds())
        assert policy.matches(DbtDatabaseError(_BENIGN_MSG)) is None

    def test_attempts_map_to_max_retries(self):
        policy = MessageRetryPolicy.for_job_retry(_JobCreds(job_retry_max_attempts=3))
        assert policy.max_retries == 2

    def test_disabled_flag_produces_disabled_policy(self):
        policy = MessageRetryPolicy.for_job_retry(_JobCreds(enable_job_retry=False))
        assert not policy.enabled
        assert policy.matches(DbtDatabaseError(_VERTEX_TIMEOUT_MSG)) is None

    def test_user_patterns_merged_with_builtins(self):
        policy = MessageRetryPolicy.for_job_retry(
            _JobCreds(job_retry_on_messages=["re:E_TRANSIENT_\\d+", "Flaky substring"])
        )
        assert policy.matches(DbtDatabaseError("boom E_TRANSIENT_42")) is not None
        assert policy.matches(DbtDatabaseError("a Flaky substring here")) is not None
        assert policy.matches(DbtDatabaseError(_VERTEX_TIMEOUT_MSG)) is not None

    def test_backoff_defaults_are_job_scaled(self):
        policy = MessageRetryPolicy.for_job_retry(_JobCreds())
        assert policy.initial_wait_seconds == 30.0
        assert policy.max_wait_seconds == 300.0

    def test_rejects_zero_attempts(self):
        with pytest.raises(ValueError, match="job_retry_max_attempts must be >= 1"):
            MessageRetryPolicy.for_job_retry(_JobCreds(job_retry_max_attempts=0))

    def test_rejects_initial_greater_than_max(self):
        with pytest.raises(ValueError, match="must be <="):
            MessageRetryPolicy.for_job_retry(
                _JobCreds(job_retry_initial_wait_seconds=500, job_retry_max_wait_seconds=10)
            )

    def test_rejects_empty_user_pattern(self):
        with pytest.raises(ValueError, match="non-empty strings"):
            MessageRetryPolicy.for_job_retry(_JobCreds(job_retry_on_messages=[""]))

    def test_resubmit_on_match_then_succeed(self):
        policy = MessageRetryPolicy.for_job_retry(_JobCreds())
        calls = {"n": 0}

        def op():
            calls["n"] += 1
            if calls["n"] == 1:
                raise DbtDatabaseError(_VERTEX_TIMEOUT_MSG)
            return "ok"

        result = retry_on_message(op, policy=policy, label="job", sleep=lambda _s: None)
        assert result == "ok"
        assert calls["n"] == 2

    def test_no_retry_on_benign_failure(self):
        policy = MessageRetryPolicy.for_job_retry(_JobCreds())
        calls = {"n": 0}

        def op():
            calls["n"] += 1
            raise DbtDatabaseError(_BENIGN_MSG)

        with pytest.raises(DbtDatabaseError):
            retry_on_message(op, policy=policy, label="job", sleep=lambda _s: None)
        assert calls["n"] == 1
