"""Unit tests for TriggerConfig parsing and validation."""

from __future__ import annotations

from datetime import timedelta

import pytest
from dbt_common.exceptions import DbtRuntimeError

from dbt.adapters.scope.trigger_config import TriggerConfig, parse_trigger_config


class TestParseDefault:
    """Default trigger when no config is provided."""

    def test_none_returns_available_now(self) -> None:
        tc = parse_trigger_config(None)
        assert tc.type == "available_now"
        assert tc.interval == timedelta(0)
        assert tc.max_cycles is None

    def test_empty_dict_returns_available_now(self) -> None:
        tc = parse_trigger_config({})
        assert tc.type == "available_now"


class TestParseAvailableNow:
    """Explicit available_now trigger."""

    def test_explicit_available_now(self) -> None:
        tc = parse_trigger_config({"type": "available_now"})
        assert tc.type == "available_now"
        assert tc.interval == timedelta(0)
        assert tc.max_cycles is None


class TestParseProcessingTime:
    """Processing time trigger with interval."""

    def test_seconds_long_form(self) -> None:
        tc = parse_trigger_config({"type": "processing_time", "interval": "10 seconds"})
        assert tc.type == "processing_time"
        assert tc.interval == timedelta(seconds=10)
        assert tc.max_cycles is None

    def test_seconds_short_form(self) -> None:
        tc = parse_trigger_config({"type": "processing_time", "interval": "30s"})
        assert tc.interval == timedelta(seconds=30)

    def test_minutes_long_form(self) -> None:
        tc = parse_trigger_config({"type": "processing_time", "interval": "1 minute"})
        assert tc.interval == timedelta(minutes=1)

    def test_minutes_short_form(self) -> None:
        tc = parse_trigger_config({"type": "processing_time", "interval": "5m"})
        assert tc.interval == timedelta(minutes=5)

    def test_minutes_plural(self) -> None:
        tc = parse_trigger_config({"type": "processing_time", "interval": "2 minutes"})
        assert tc.interval == timedelta(minutes=2)

    def test_secs_form(self) -> None:
        tc = parse_trigger_config({"type": "processing_time", "interval": "15 secs"})
        assert tc.interval == timedelta(seconds=15)

    def test_sec_form(self) -> None:
        tc = parse_trigger_config({"type": "processing_time", "interval": "1sec"})
        assert tc.interval == timedelta(seconds=1)

    def test_mins_form(self) -> None:
        tc = parse_trigger_config({"type": "processing_time", "interval": "3mins"})
        assert tc.interval == timedelta(minutes=3)

    def test_min_form(self) -> None:
        tc = parse_trigger_config({"type": "processing_time", "interval": "1min"})
        assert tc.interval == timedelta(minutes=1)

    def test_case_insensitive(self) -> None:
        tc = parse_trigger_config({"type": "processing_time", "interval": "10 Seconds"})
        assert tc.interval == timedelta(seconds=10)

    def test_with_extra_whitespace(self) -> None:
        tc = parse_trigger_config({"type": "processing_time", "interval": "  10  seconds  "})
        assert tc.interval == timedelta(seconds=10)


class TestParseMaxCycles:
    """Max cycles parsing."""

    def test_with_max_cycles(self) -> None:
        tc = parse_trigger_config(
            {
                "type": "processing_time",
                "interval": "30s",
                "max_cycles": 5,
            }
        )
        assert tc.max_cycles == 5

    def test_max_cycles_as_string(self) -> None:
        tc = parse_trigger_config(
            {
                "type": "processing_time",
                "interval": "10s",
                "max_cycles": "100",
            }
        )
        assert tc.max_cycles == 100

    def test_max_cycles_none_means_infinite(self) -> None:
        tc = parse_trigger_config(
            {
                "type": "processing_time",
                "interval": "10s",
            }
        )
        assert tc.max_cycles is None


class TestValidationErrors:
    """Error cases."""

    def test_invalid_type(self) -> None:
        with pytest.raises(DbtRuntimeError, match="Invalid trigger type 'invalid'"):
            parse_trigger_config({"type": "invalid"})

    def test_missing_interval_for_processing_time(self) -> None:
        with pytest.raises(DbtRuntimeError, match="requires an 'interval'"):
            parse_trigger_config({"type": "processing_time"})

    def test_empty_interval_for_processing_time(self) -> None:
        with pytest.raises(DbtRuntimeError, match="requires an 'interval'"):
            parse_trigger_config({"type": "processing_time", "interval": ""})

    def test_negative_interval(self) -> None:
        with pytest.raises(DbtRuntimeError, match="must be positive"):
            parse_trigger_config({"type": "processing_time", "interval": "-5 seconds"})

    def test_zero_interval(self) -> None:
        with pytest.raises(DbtRuntimeError, match="must be positive"):
            parse_trigger_config({"type": "processing_time", "interval": "0 seconds"})

    def test_invalid_interval_format(self) -> None:
        with pytest.raises(DbtRuntimeError, match="Invalid trigger interval"):
            parse_trigger_config({"type": "processing_time", "interval": "ten seconds"})

    def test_negative_max_cycles(self) -> None:
        with pytest.raises(DbtRuntimeError, match="must be a positive integer"):
            parse_trigger_config(
                {
                    "type": "processing_time",
                    "interval": "10s",
                    "max_cycles": -1,
                }
            )

    def test_zero_max_cycles(self) -> None:
        with pytest.raises(DbtRuntimeError, match="must be a positive integer"):
            parse_trigger_config(
                {
                    "type": "processing_time",
                    "interval": "10s",
                    "max_cycles": 0,
                }
            )

    def test_non_numeric_max_cycles(self) -> None:
        with pytest.raises(DbtRuntimeError, match="must be an integer"):
            parse_trigger_config(
                {
                    "type": "processing_time",
                    "interval": "10s",
                    "max_cycles": "abc",
                }
            )


class TestTriggerConfigFrozen:
    """TriggerConfig is immutable."""

    def test_frozen(self) -> None:
        tc = TriggerConfig(type="available_now")
        with pytest.raises(AttributeError):
            tc.type = "processing_time"  # type: ignore[misc]
