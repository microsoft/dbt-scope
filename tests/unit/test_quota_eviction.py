"""Unit tests for the quota-eviction recovery layer."""

from __future__ import annotations

import threading
from typing import Any
from unittest.mock import MagicMock

import pytest

from dbt.adapters.scope.quota_eviction import (
    _EVICTION_LOCKS,
    QuotaEvictionPolicy,
    _lock_for,
    is_quota_error,
    retry_with_quota_eviction,
    select_victims,
)


class _Creds:
    def __init__(
        self,
        *,
        adla_account: str = "acct",
        enable_quota_eviction: bool = True,
        quota_eviction_max_attempts: int = 25,
        quota_eviction_cancel_num: int = 5,
        quota_eviction_wait_seconds: float = 30.0,
        quota_eviction_jitter_seconds: float = 5.0,
    ) -> None:
        self.adla_account = adla_account
        self.enable_quota_eviction = enable_quota_eviction
        self.quota_eviction_max_attempts = quota_eviction_max_attempts
        self.quota_eviction_cancel_num = quota_eviction_cancel_num
        self.quota_eviction_wait_seconds = quota_eviction_wait_seconds
        self.quota_eviction_jitter_seconds = quota_eviction_jitter_seconds


class _FakeCtx:
    def __init__(self, jobs: list[dict[str, Any]]) -> None:
        self.jobs = jobs
        self.list_calls: list[tuple[str | None, int]] = []
        self.cancel_calls: list[str] = []
        self.cancel_failures: dict[str, Exception] = {}

    def list_jobs(self, filter_expr: str | None = None, top: int = 100) -> list[dict[str, Any]]:
        self.list_calls.append((filter_expr, top))
        return list(self.jobs)

    def cancel_job_async(self, job_id: str) -> None:
        self.cancel_calls.append(job_id)
        if job_id in self.cancel_failures:
            raise self.cancel_failures[job_id]


class TestQuotaEvictionPolicyConstruction:
    def test_from_credentials_uses_provided_values(self):
        creds = _Creds(
            adla_account="my-acct",
            enable_quota_eviction=True,
            quota_eviction_max_attempts=7,
            quota_eviction_cancel_num=3,
            quota_eviction_wait_seconds=12.5,
            quota_eviction_jitter_seconds=1.5,
        )
        policy = QuotaEvictionPolicy.from_credentials(creds)
        assert policy.account == "my-acct"
        assert policy.enabled is True
        assert policy.max_attempts == 7
        assert policy.cancel_num == 3
        assert policy.wait_seconds == 12.5
        assert policy.jitter_seconds == 1.5

    def test_from_credentials_respects_disabled_flag(self):
        policy = QuotaEvictionPolicy.from_credentials(_Creds(enable_quota_eviction=False))
        assert policy.enabled is False

    def test_disabled_factory(self):
        policy = QuotaEvictionPolicy.disabled()
        assert policy.enabled is False
        assert policy.max_attempts == 0

    def test_policy_is_frozen(self):
        policy = QuotaEvictionPolicy.disabled()
        with pytest.raises(AttributeError):
            policy.max_attempts = 99  # type: ignore[misc]


class TestIsQuotaError:
    def test_matches_production_error(self):
        msg = (
            "ADLA API PUT https://x.azuredatalakeanalytics.net/jobs/abc returned 400: "
            '{"Message":"Cannot exceed 1000 queued SCOPE jobs in an ADLA workspace."}'
        )
        assert is_quota_error(RuntimeError(msg)) is True

    def test_matches_with_different_quota_number(self):
        assert is_quota_error(RuntimeError("Cannot exceed 250 queued SCOPE jobs")) is True

    def test_rejects_unrelated_error(self):
        assert is_quota_error(RuntimeError("Internal server error")) is False

    def test_rejects_partial_match_first_needle_only(self):
        assert is_quota_error(RuntimeError("Cannot exceed budget")) is False

    def test_rejects_partial_match_second_needle_only(self):
        assert is_quota_error(RuntimeError("queued SCOPE jobs reported")) is False


class TestSelectVictims:
    def test_sorts_highest_priority_number_first(self):
        jobs = [
            {"jobId": "j1", "priority": 1, "submitTime": "2026-01-01T00:00:00Z"},
            {"jobId": "j2", "priority": 5, "submitTime": "2026-01-01T00:00:00Z"},
            {"jobId": "j3", "priority": 3, "submitTime": "2026-01-01T00:00:00Z"},
        ]
        victims = select_victims(jobs, k=3)
        assert [v["jobId"] for v in victims] == ["j2", "j3", "j1"]

    def test_within_priority_tier_oldest_first(self):
        jobs = [
            {"jobId": "j1", "priority": 9, "submitTime": "2026-02-01T00:00:00Z"},
            {"jobId": "j2", "priority": 9, "submitTime": "2026-01-01T00:00:00Z"},
            {"jobId": "j3", "priority": 9, "submitTime": "2026-03-01T00:00:00Z"},
        ]
        victims = select_victims(jobs, k=3)
        assert [v["jobId"] for v in victims] == ["j2", "j1", "j3"]

    def test_respects_k(self):
        jobs = [{"jobId": f"j{i}", "priority": i, "submitTime": ""} for i in range(10)]
        victims = select_victims(jobs, k=3)
        assert len(victims) == 3
        assert [v["jobId"] for v in victims] == ["j9", "j8", "j7"]

    def test_returns_fewer_when_list_shorter_than_k(self):
        jobs = [{"jobId": "j1", "priority": 1, "submitTime": ""}]
        victims = select_victims(jobs, k=5)
        assert len(victims) == 1

    def test_empty_list_returns_empty(self):
        assert select_victims([], k=5) == []

    def test_zero_k_returns_empty(self):
        jobs = [{"jobId": "j1", "priority": 1, "submitTime": ""}]
        assert select_victims(jobs, k=0) == []

    def test_handles_missing_priority(self):
        jobs = [
            {"jobId": "j1", "submitTime": "2026-01-01T00:00:00Z"},
            {"jobId": "j2", "priority": 5, "submitTime": "2026-01-01T00:00:00Z"},
        ]
        victims = select_victims(jobs, k=2)
        assert victims[0]["jobId"] == "j2"

    def test_handles_non_numeric_priority(self):
        jobs = [
            {"jobId": "j1", "priority": "not-a-number", "submitTime": ""},
            {"jobId": "j2", "priority": 3, "submitTime": ""},
        ]
        victims = select_victims(jobs, k=2)
        assert victims[0]["jobId"] == "j2"


class TestLockFor:
    def setup_method(self):
        _EVICTION_LOCKS.clear()

    def test_same_account_returns_same_lock(self):
        assert _lock_for("a") is _lock_for("a")

    def test_different_accounts_get_different_locks(self):
        assert _lock_for("a") is not _lock_for("b")

    def test_locks_dict_thread_safe_under_contention(self):
        results: list[threading.Lock] = []
        barrier = threading.Barrier(20)

        def grab() -> None:
            barrier.wait()
            results.append(_lock_for("shared"))

        threads = [threading.Thread(target=grab) for _ in range(20)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        assert len({id(r) for r in results}) == 1


class TestRetryWithQuotaEviction:
    def setup_method(self):
        _EVICTION_LOCKS.clear()

    def _policy(self, **overrides: Any) -> QuotaEvictionPolicy:
        defaults = {
            "account": "test-acct",
            "enabled": True,
            "max_attempts": 3,
            "cancel_num": 2,
            "wait_seconds": 30.0,
            "jitter_seconds": 5.0,
        }
        defaults.update(overrides)
        return QuotaEvictionPolicy(**defaults)

    def test_happy_path_succeeds_without_eviction(self):
        ctx = _FakeCtx(jobs=[])
        op = MagicMock(return_value={"ok": True})
        result = retry_with_quota_eviction(
            op, eviction_ctx=ctx, policy=self._policy(), label="test"
        )
        assert result == {"ok": True}
        assert ctx.cancel_calls == []
        assert ctx.list_calls == []
        assert op.call_count == 1

    def test_disabled_policy_passes_through(self):
        ctx = _FakeCtx(jobs=[])
        err = RuntimeError("Cannot exceed 1000 queued SCOPE jobs")
        op = MagicMock(side_effect=err)
        with pytest.raises(RuntimeError, match="Cannot exceed"):
            retry_with_quota_eviction(
                op,
                eviction_ctx=ctx,
                policy=QuotaEvictionPolicy.disabled(),
                label="test",
            )
        assert ctx.cancel_calls == []
        assert op.call_count == 1

    def test_non_quota_error_reraises_immediately(self):
        ctx = _FakeCtx(jobs=[{"jobId": "j1", "priority": 9, "submitTime": ""}])
        op = MagicMock(side_effect=RuntimeError("unrelated 500 error"))
        with pytest.raises(RuntimeError, match="unrelated"):
            retry_with_quota_eviction(op, eviction_ctx=ctx, policy=self._policy(), label="test")
        assert ctx.cancel_calls == []
        assert op.call_count == 1

    def test_one_eviction_recovery(self):
        jobs = [{"jobId": f"j{i}", "priority": i, "submitTime": ""} for i in range(1, 11)]
        ctx = _FakeCtx(jobs=jobs)
        err = RuntimeError("Cannot exceed 1000 queued SCOPE jobs")
        op = MagicMock(side_effect=[err, {"ok": True}])
        sleeps: list[float] = []

        result = retry_with_quota_eviction(
            op,
            eviction_ctx=ctx,
            policy=self._policy(cancel_num=3),
            label="test",
            sleep=lambda s: sleeps.append(s),
            random_uniform=lambda a, b: 0.0,
        )
        assert result == {"ok": True}
        assert op.call_count == 2
        assert ctx.list_calls == [("state ne 'Ended'", 1000)]
        assert ctx.cancel_calls == ["j10", "j9", "j8"]
        assert sleeps == [30.0]

    def test_exhausts_max_attempts(self):
        jobs = [{"jobId": "j1", "priority": 9, "submitTime": ""}]
        ctx = _FakeCtx(jobs=jobs)
        err = RuntimeError("Cannot exceed 999 queued SCOPE jobs forever")
        op = MagicMock(side_effect=err)

        with pytest.raises(RuntimeError, match="Cannot exceed"):
            retry_with_quota_eviction(
                op,
                eviction_ctx=ctx,
                policy=self._policy(max_attempts=2),
                label="test",
                sleep=lambda s: None,
                random_uniform=lambda a, b: 0.0,
            )
        assert op.call_count == 2
        assert len(ctx.cancel_calls) == 2

    def test_empty_job_list_reraises(self):
        ctx = _FakeCtx(jobs=[])
        err = RuntimeError("Cannot exceed 1000 queued SCOPE jobs")
        op = MagicMock(side_effect=err)
        with pytest.raises(RuntimeError, match="Cannot exceed"):
            retry_with_quota_eviction(
                op,
                eviction_ctx=ctx,
                policy=self._policy(),
                label="test",
                sleep=lambda s: None,
                random_uniform=lambda a, b: 0.0,
            )
        assert op.call_count == 1

    def test_individual_cancel_failure_swallowed(self):
        jobs = [{"jobId": f"j{i}", "priority": i, "submitTime": ""} for i in range(1, 4)]
        ctx = _FakeCtx(jobs=jobs)
        ctx.cancel_failures["j3"] = RuntimeError("transient cancel 500")

        err = RuntimeError("Cannot exceed 1000 queued SCOPE jobs")
        op = MagicMock(side_effect=[err, {"ok": True}])

        result = retry_with_quota_eviction(
            op,
            eviction_ctx=ctx,
            policy=self._policy(cancel_num=3),
            label="test",
            sleep=lambda s: None,
            random_uniform=lambda a, b: 0.0,
        )
        assert result == {"ok": True}
        assert set(ctx.cancel_calls) == {"j1", "j2", "j3"}

    def test_list_jobs_failure_aborts(self):
        ctx = _FakeCtx(jobs=[])
        ctx.list_jobs = MagicMock(side_effect=RuntimeError("list failed"))  # type: ignore[method-assign]
        err = RuntimeError("Cannot exceed 1000 queued SCOPE jobs")
        op = MagicMock(side_effect=err)

        with pytest.raises(RuntimeError, match="Cannot exceed"):
            retry_with_quota_eviction(
                op,
                eviction_ctx=ctx,
                policy=self._policy(),
                label="test",
                sleep=lambda s: None,
                random_uniform=lambda a, b: 0.0,
            )
        assert op.call_count == 1

    def test_jitter_applied_to_sleep(self):
        jobs = [{"jobId": "j1", "priority": 9, "submitTime": ""}]
        ctx = _FakeCtx(jobs=jobs)
        err = RuntimeError("Cannot exceed 1000 queued SCOPE jobs")
        op = MagicMock(side_effect=[err, {"ok": True}])
        sleeps: list[float] = []

        retry_with_quota_eviction(
            op,
            eviction_ctx=ctx,
            policy=self._policy(wait_seconds=30, jitter_seconds=5),
            label="test",
            sleep=lambda s: sleeps.append(s),
            random_uniform=lambda a, b: 3.5,
        )
        assert sleeps == [33.5]

    def test_negative_jitter_clamped_to_zero(self):
        jobs = [{"jobId": "j1", "priority": 9, "submitTime": ""}]
        ctx = _FakeCtx(jobs=jobs)
        err = RuntimeError("Cannot exceed 1000 queued SCOPE jobs")
        op = MagicMock(side_effect=[err, {"ok": True}])
        sleeps: list[float] = []

        retry_with_quota_eviction(
            op,
            eviction_ctx=ctx,
            policy=self._policy(wait_seconds=2, jitter_seconds=10),
            label="test",
            sleep=lambda s: sleeps.append(s),
            random_uniform=lambda a, b: -100,
        )
        assert sleeps == [0.0]
