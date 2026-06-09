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
        compiled: list[Any] = []
        for entry in raw_patterns:
            if not isinstance(entry, str) or not entry:
                raise ValueError(
                    f"retry_on_error_messages entries must be non-empty strings; got {entry!r}"
                )
            if entry.startswith(_REGEX_PREFIX):
                pattern_text = entry[len(_REGEX_PREFIX) :]
                if not pattern_text:
                    raise ValueError(
                        f"retry_on_error_messages regex entry {entry!r} is empty after 're:'"
                    )
                compiled.append(re.compile(pattern_text))
            else:
                compiled.append(entry)

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
