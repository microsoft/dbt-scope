"""Quota-eviction recovery layer for ADLA SCOPE job submission.

When the ADLA workspace queue is saturated, ``submit_job`` fails with a
400 BadRequest carrying::

    Cannot exceed 1000 queued SCOPE jobs in an ADLA workspace.

This layer intercepts that specific error, lists every non-terminal job in
the workspace, picks the least-important (highest ``priority`` number),
oldest victims, cancels ``cancel_num`` of them, sleeps with jitter, and
retries the original submit. A per-account ``threading.Lock`` prevents
concurrent dbt threads from cascading evictions for the same quota event.

Configured via ``ScopeCredentials``::

    enable_quota_eviction: true
    quota_eviction_max_attempts: 25
    quota_eviction_cancel_num: 5
    quota_eviction_wait_seconds: 30
    quota_eviction_jitter_seconds: 5
"""

from __future__ import annotations

import random
import threading
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, Protocol, TypeVar

from dbt.adapters.events.logging import AdapterLogger

log = AdapterLogger("scope")

T = TypeVar("T")

_QUOTA_NEEDLES = ("Cannot exceed", "queued SCOPE jobs")
_NON_TERMINAL_FILTER = "state ne 'Ended'"
_LIST_TOP = 1000

_EVICTION_LOCKS: dict[str, threading.Lock] = {}
_EVICTION_LOCKS_GUARD = threading.Lock()


def _lock_for(account: str) -> threading.Lock:
    with _EVICTION_LOCKS_GUARD:
        lk = _EVICTION_LOCKS.get(account)
        if lk is None:
            lk = threading.Lock()
            _EVICTION_LOCKS[account] = lk
        return lk


@dataclass(frozen=True)
class QuotaEvictionPolicy:
    """Eviction recovery policy for ADLA queue-saturation 400s."""

    account: str
    enabled: bool
    max_attempts: int
    cancel_num: int
    wait_seconds: float
    jitter_seconds: float

    @classmethod
    def from_credentials(cls, credentials: Any) -> QuotaEvictionPolicy:
        return cls(
            account=getattr(credentials, "adla_account", "") or "",
            enabled=bool(getattr(credentials, "enable_quota_eviction", True)),
            max_attempts=int(getattr(credentials, "quota_eviction_max_attempts", 25)),
            cancel_num=int(getattr(credentials, "quota_eviction_cancel_num", 5)),
            wait_seconds=float(getattr(credentials, "quota_eviction_wait_seconds", 30.0)),
            jitter_seconds=float(getattr(credentials, "quota_eviction_jitter_seconds", 5.0)),
        )

    @classmethod
    def disabled(cls) -> QuotaEvictionPolicy:
        return cls(
            account="",
            enabled=False,
            max_attempts=0,
            cancel_num=1,
            wait_seconds=1.0,
            jitter_seconds=0.0,
        )


class EvictionContext(Protocol):
    """Operations the eviction layer needs from the connection handle.

    ``cancel_job_async`` MUST be fire-and-forget — it must NOT block waiting
    for the job to reach a terminal state. Blocking would multiply the
    recovery time by the number of victims.

    ``is_self_job`` lets the eviction layer skip jobs that belong to the
    current dbt run (so we never cancel our own in-flight work — issue #39).
    """

    def list_jobs(self, filter_expr: str | None = None, top: int = 100) -> list[dict[str, Any]]: ...

    def cancel_job_async(self, job_id: str) -> None: ...

    def is_self_job(self, job: dict[str, Any]) -> bool: ...


def is_quota_error(exc: BaseException) -> bool:
    """Return True when ``exc`` carries the ADLA queue-saturation message."""
    msg = str(exc)
    return all(needle in msg for needle in _QUOTA_NEEDLES)


def select_victims(jobs: list[dict[str, Any]], k: int) -> list[dict[str, Any]]:
    """Return the top-``k`` eviction victims.

    Sorts by highest ``priority`` number (least important) first, then by
    oldest ``submitTime`` (lexical ISO-8601 sort). Missing fields are
    treated as worst-case (priority=0 = most important; submitTime='' =
    oldest, so an unknown timestamp gets evicted earlier within a tier).
    """
    if k <= 0 or not jobs:
        return []

    def sort_key(job: dict[str, Any]) -> tuple[int, str]:
        raw_priority = job.get("priority", 0)
        try:
            priority = int(raw_priority)
        except (TypeError, ValueError):
            priority = 0
        submit_time = job.get("submitTime") or ""
        return (-priority, str(submit_time))

    ordered = sorted(jobs, key=sort_key)
    return ordered[:k]


def retry_with_quota_eviction(
    op: Callable[[], T],
    *,
    eviction_ctx: EvictionContext,
    policy: QuotaEvictionPolicy,
    label: str,
    sleep: Callable[[float], None] = time.sleep,
    random_uniform: Callable[[float, float], float] = random.uniform,
) -> T:
    """Execute ``op``; on ADLA quota 400, evict and retry up to ``max_attempts`` times."""
    if not policy.enabled or policy.max_attempts <= 0:
        return op()

    lock = _lock_for(policy.account)
    last_exc: BaseException | None = None

    for attempt in range(1, policy.max_attempts + 1):
        try:
            return op()
        except Exception as exc:
            if not is_quota_error(exc):
                raise
            last_exc = exc
            log.warning(
                f"{label}: ADLA quota hit ({exc}). "
                f"Attempt {attempt}/{policy.max_attempts}: "
                f"evicting up to {policy.cancel_num} job(s) and retrying."
            )

            with lock:
                victims = _gather_and_cancel(eviction_ctx, policy)

            if not victims:
                log.warning(
                    f"{label}: no eviction candidates found in workspace "
                    f"'{policy.account}'; aborting retry."
                )
                raise

            jitter = random_uniform(-policy.jitter_seconds, policy.jitter_seconds)
            delay = max(0.0, policy.wait_seconds + jitter)
            log.info(
                f"{label}: cancelled {len(victims)} victim(s); sleeping {delay:.1f}s before retry."
            )
            sleep(delay)

    assert last_exc is not None
    log.warning(f"{label}: exhausted {policy.max_attempts} quota-eviction attempts; raising.")
    raise last_exc


def _gather_and_cancel(ctx: EvictionContext, policy: QuotaEvictionPolicy) -> list[dict[str, Any]]:
    try:
        jobs = ctx.list_jobs(filter_expr=_NON_TERMINAL_FILTER, top=_LIST_TOP)
    except Exception as exc:
        log.warning(f"Quota eviction: list_jobs failed ({exc}); cannot pick victims.")
        return []

    # Never evict jobs belonging to the current dbt run.
    total = len(jobs)
    jobs = [job for job in jobs if not ctx.is_self_job(job)]
    skipped = total - len(jobs)
    if skipped:
        log.debug(f"Quota eviction: excluded {skipped} self-owned job(s) from eviction.")

    victims = select_victims(jobs, policy.cancel_num)
    if not victims:
        return []

    cancelled: list[dict[str, Any]] = []
    for victim in victims:
        job_id = str(victim.get("jobId") or "")
        if not job_id:
            continue
        name = victim.get("name", "<unknown>")
        priority = victim.get("priority", "?")
        submit_time = victim.get("submitTime", "?")
        log.info(
            f"Quota eviction: cancelling job {job_id} "
            f"(name='{name}', priority={priority}, submitTime={submit_time})"
        )
        try:
            ctx.cancel_job_async(job_id)
            cancelled.append(victim)
        except Exception as exc:
            log.warning(f"Quota eviction: cancel of {job_id} failed ({exc}); continuing.")

    return cancelled
