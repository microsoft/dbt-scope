"""Trigger configuration for Spark Structured Streaming-style trigger modes.

Supports two trigger types:
- ``available_now``: process all available files, then exit (default, backwards-compatible).
- ``processing_time``: continuously loop — discover → batch → sleep → repeat.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import timedelta

from dbt_common.exceptions import DbtRuntimeError

# Pattern: optional sign, digits, optional whitespace, unit
_INTERVAL_RE = re.compile(
    r"^\s*(?P<value>-?\d+(?:\.\d+)?)\s*(?P<unit>s|sec|secs|second|seconds|m|min|mins|minute|minutes)\s*$",
    re.IGNORECASE,
)

_UNIT_TO_SECONDS: dict[str, float] = {
    "s": 1,
    "sec": 1,
    "secs": 1,
    "second": 1,
    "seconds": 1,
    "m": 60,
    "min": 60,
    "mins": 60,
    "minute": 60,
    "minutes": 60,
}

VALID_TRIGGER_TYPES = frozenset({"available_now", "processing_time"})


def _parse_interval(raw: str) -> timedelta:
    """Parse a human-readable interval string into a :class:`timedelta`.

    Accepted formats: ``'10 seconds'``, ``'30s'``, ``'1 minute'``, ``'5m'``, etc.
    """
    match = _INTERVAL_RE.match(raw)
    if not match:
        raise DbtRuntimeError(
            f"Invalid trigger interval '{raw}'. "
            f"Expected a format like '10 seconds', '30s', '1 minute', '5m'."
        )

    value = float(match.group("value"))
    unit = match.group("unit").lower()
    seconds = value * _UNIT_TO_SECONDS[unit]

    if seconds <= 0:
        raise DbtRuntimeError(f"Trigger interval must be positive, got '{raw}' ({seconds}s).")

    return timedelta(seconds=seconds)


@dataclass(frozen=True)
class TriggerConfig:
    """Parsed trigger configuration for a dbt model."""

    type: str
    interval: timedelta = timedelta(0)
    max_cycles: int | None = None

    def __post_init__(self) -> None:
        if self.type not in VALID_TRIGGER_TYPES:
            raise DbtRuntimeError(
                f"Invalid trigger type '{self.type}'. "
                f"Must be one of: {', '.join(sorted(VALID_TRIGGER_TYPES))}."
            )
        if self.type == "processing_time" and self.interval.total_seconds() <= 0:
            raise DbtRuntimeError("Trigger type 'processing_time' requires a positive 'interval'.")
        if self.max_cycles is not None and self.max_cycles <= 0:
            raise DbtRuntimeError(f"max_cycles must be a positive integer, got {self.max_cycles}.")


def parse_trigger_config(raw: dict | None) -> TriggerConfig:
    """Parse a trigger config dict from dbt model ``config()``.

    Returns the default ``available_now`` trigger if *raw* is ``None`` or empty.
    """
    if not raw:
        return TriggerConfig(type="available_now")

    trigger_type = raw.get("type", "available_now")

    interval = timedelta(0)
    if trigger_type == "processing_time":
        raw_interval = raw.get("interval")
        if not raw_interval:
            raise DbtRuntimeError(
                "Trigger type 'processing_time' requires an 'interval' "
                "(e.g. '10 seconds', '30s', '1 minute')."
            )
        interval = _parse_interval(str(raw_interval))

    max_cycles = raw.get("max_cycles")
    if max_cycles is not None:
        try:
            max_cycles = int(max_cycles)
        except (ValueError, TypeError) as exc:
            raise DbtRuntimeError(f"max_cycles must be an integer, got '{max_cycles}'.") from exc

    return TriggerConfig(type=trigger_type, interval=interval, max_cycles=max_cycles)
