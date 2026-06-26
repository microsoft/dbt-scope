"""Message-pattern-based retry layer for Azure API calls.

Sits above the urllib3 transport retry (which handles 429 / 5xx + connect/read
errors) and above the ``RetryPolicy`` token retry (which handles
``CredentialUnavailableError``). This layer inspects the **exception message**
of whatever leaks past those — typically structured error bodies returned by
ADLA / ADLS as 4xx responses — and retries with bounded exponential backoff
when the message matches one of the user-configured patterns.

Configured via ``ScopeCredentials``::

    retry_on_error_messages:
      - "Cannot exceed"           # plain substring (case-sensitive)
      - " queued SCOPE jobs"      # plain substring
      - "re:Cannot exceed \\d+"   # compiled as regex when prefixed with "re:"
    max_retries_on_error: 25
    max_wait_on_error_seconds: 30
    initial_wait_on_error_seconds: 1
"""

from __future__ import annotations

import re
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, TypeVar

from dbt.adapters.events.logging import AdapterLogger

log = AdapterLogger("scope")

T = TypeVar("T")

_REGEX_PREFIX = "re:"


@dataclass(frozen=True)
class RetryRule:
    """A single regex-based retry rule for transient SCOPE job failures.

    ``DEFAULT_JOB_RETRY_RULES`` below is the curated, growable rule set. To add
    coverage for a newly-observed transient error, append a ``RetryRule`` with a
    conservative regex and a short description of why it is safe to retry.
    """

    name: str
    pattern: str
    description: str


# Built-in retry rules for SCOPE *job* failures (re-submit the whole job when the
# terminal error message matches). Keep patterns conservative — broad patterns
# risk re-running a genuinely-failed job. Grow this tuple over time.
DEFAULT_JOB_RETRY_RULES: tuple[RetryRule, ...] = (
    RetryRule(
        name="vertex_stream_open_timeout",
        pattern=r"Exception in VertexManager.*Failed to open stream.*Operation timed out",
        description=("Transient DMS/Cosmos stream-open timeout in a vertex."),
    ),
    RetryRule(
        name="job_cancelled_by_user",
        pattern=r"Job cancelled by user .*",
        description=(
            "Job cancelled by an external actor (e.g. another pipeline's quota "
            "eviction). Our own run's jobs are excluded from eviction separately "
        ),
    ),
)


def _compile_patterns(raw_patterns: list[Any]) -> list[Any]:
    """Compile a list of pattern entries into substrings / ``re.Pattern`` objects.

    Entries prefixed with ``re:`` are compiled as regexes; all others are kept as
    plain (case-sensitive) substrings. Raises ``ValueError`` on malformed entries.
    """
    compiled: list[Any] = []
    for entry in raw_patterns:
        if not isinstance(entry, str) or not entry:
            raise ValueError(f"retry pattern entries must be non-empty strings; got {entry!r}")
        if entry.startswith(_REGEX_PREFIX):
            pattern_text = entry[len(_REGEX_PREFIX) :]
            if not pattern_text:
                raise ValueError(f"retry regex entry {entry!r} is empty after 're:'")
            compiled.append(re.compile(pattern_text))
        else:
            compiled.append(entry)
    return compiled


@dataclass(frozen=True)
class MessageRetryPolicy:
    """Exponential-backoff retry triggered by exception message patterns.

    ``max_retries`` is the number of additional attempts AFTER the first try
    (matching the semantics of ``urllib3.Retry(total=...)``). Total attempts
    == ``max_retries + 1``.

    Delay between attempts is ``min(initial_wait_seconds * 2**(attempt-1),
    max_wait_seconds)`` (capped exponential, no jitter — deterministic for
    testing). Empty ``patterns`` disables the layer entirely.
    """

    patterns: tuple[Any, ...] = ()
    max_retries: int = 25
    initial_wait_seconds: float = 1.0
    max_wait_seconds: float = 30.0

    @classmethod
    def disabled(cls) -> MessageRetryPolicy:
        return cls(patterns=())

    @classmethod
    def from_credentials(cls, credentials: Any) -> MessageRetryPolicy:
        raw_patterns = getattr(credentials, "retry_on_error_messages", None) or []
        compiled = _compile_patterns(raw_patterns)

        max_retries = int(getattr(credentials, "max_retries_on_error", 25))
        initial_wait = float(getattr(credentials, "initial_wait_on_error_seconds", 1.0))
        max_wait = float(getattr(credentials, "max_wait_on_error_seconds", 30.0))

        if max_retries < 0:
            raise ValueError(f"max_retries_on_error must be >= 0; got {max_retries}")
        if initial_wait <= 0:
            raise ValueError(f"initial_wait_on_error_seconds must be > 0; got {initial_wait}")
        if max_wait <= 0:
            raise ValueError(f"max_wait_on_error_seconds must be > 0; got {max_wait}")
        if initial_wait > max_wait:
            raise ValueError(
                "initial_wait_on_error_seconds must be <= max_wait_on_error_seconds; "
                f"got {initial_wait} > {max_wait}"
            )

        return cls(
            patterns=tuple(compiled),
            max_retries=max_retries,
            initial_wait_seconds=initial_wait,
            max_wait_seconds=max_wait,
        )

    @classmethod
    def for_job_retry(cls, credentials: Any) -> MessageRetryPolicy:
        """Build the policy that re-submits a SCOPE *job* on a transient failure.

        Combines the built-in ``DEFAULT_JOB_RETRY_RULES`` with any user-supplied
        ``job_retry_on_messages`` (same ``re:`` / substring syntax as
        ``retry_on_error_messages``). Returns a disabled policy when
        ``enable_job_retry`` is false so callers run the job exactly once.

        Config (all optional, with defaults):
          ``enable_job_retry`` (True), ``job_retry_on_messages`` ([]),
          ``job_retry_max_attempts`` (3 total attempts),
          ``job_retry_initial_wait_seconds`` (30), ``job_retry_max_wait_seconds`` (300).
        """
        if not bool(getattr(credentials, "enable_job_retry", True)):
            return cls.disabled()

        user_patterns = getattr(credentials, "job_retry_on_messages", None) or []
        compiled = _compile_patterns(user_patterns)
        compiled.extend(re.compile(rule.pattern) for rule in DEFAULT_JOB_RETRY_RULES)

        max_attempts = int(getattr(credentials, "job_retry_max_attempts", 3))
        initial_wait = float(getattr(credentials, "job_retry_initial_wait_seconds", 30.0))
        max_wait = float(getattr(credentials, "job_retry_max_wait_seconds", 300.0))

        if max_attempts < 1:
            raise ValueError(f"job_retry_max_attempts must be >= 1; got {max_attempts}")
        if initial_wait <= 0:
            raise ValueError(f"job_retry_initial_wait_seconds must be > 0; got {initial_wait}")
        if max_wait <= 0:
            raise ValueError(f"job_retry_max_wait_seconds must be > 0; got {max_wait}")
        if initial_wait > max_wait:
            raise ValueError(
                "job_retry_initial_wait_seconds must be <= job_retry_max_wait_seconds; "
                f"got {initial_wait} > {max_wait}"
            )

        # MessageRetryPolicy.max_retries == additional attempts after the first.
        return cls(
            patterns=tuple(compiled),
            max_retries=max_attempts - 1,
            initial_wait_seconds=initial_wait,
            max_wait_seconds=max_wait,
        )

    @property
    def enabled(self) -> bool:
        return bool(self.patterns) and self.max_retries > 0

    def matches(self, exc: BaseException) -> str | None:
        if not self.patterns:
            return None
        text = str(exc)
        for pattern in self.patterns:
            if isinstance(pattern, re.Pattern):
                if pattern.search(text):
                    return f"re:{pattern.pattern}"
            elif pattern in text:
                return pattern
        return None

    def delay_for_attempt(self, attempt: int) -> float:
        if attempt < 1:
            attempt = 1
        raw = self.initial_wait_seconds * (2 ** (attempt - 1))
        return min(raw, self.max_wait_seconds)


def retry_on_message(
    operation: Callable[[], T],
    *,
    policy: MessageRetryPolicy,
    label: str,
    sleep: Callable[[float], None] = time.sleep,
) -> T:
    """Run ``operation``; on a pattern-matching exception, retry with backoff.

    Non-matching exceptions are re-raised immediately without delay. When the
    retry budget is exhausted the **original** exception is re-raised so
    downstream callers see the unmodified type (e.g. ``DbtDatabaseError``
    stays ``DbtDatabaseError``).
    """
    if not policy.enabled:
        return operation()

    last_exc: BaseException | None = None
    total_attempts = policy.max_retries + 1
    for attempt in range(1, total_attempts + 1):
        try:
            return operation()
        except BaseException as exc:
            matched = policy.matches(exc)
            if matched is None:
                raise
            last_exc = exc
            if attempt >= total_attempts:
                log.error(
                    f"[{label}] retry budget exhausted after {attempt} attempt(s) "
                    f"on pattern {matched!r}: {exc}"
                )
                raise
            delay = policy.delay_for_attempt(attempt)
            log.warning(
                f"[{label}] transient error matched pattern {matched!r} "
                f"(attempt {attempt}/{total_attempts}); retrying in {delay:.1f}s: {exc}"
            )
            sleep(delay)
    assert last_exc is not None
    raise last_exc
