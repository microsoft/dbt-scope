"""Root test configuration — per-test log file routing for all tests.

Every test (unit + integration) gets its own log file under ``.logs/``
so post-mortem debugging never requires re-running the suite.

    .logs/TestFullRefresh__test_full_refresh_creates_delta_partitions.log
    .logs/test_generates_create_table.log

Uses a ``pytest_runtest_protocol`` hook instead of an autouse fixture so
that the log file is created **before** any fixtures (including session-
scoped ones) run.  This guarantees capture even when a higher-scoped
fixture errors out.
"""

from __future__ import annotations

import logging
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).parent.parent
LOGS_DIR = REPO_ROOT / ".logs"

# Azure SDK loggers are extremely chatty at INFO (every HTTP request/response).
# Silence them to WARNING so they don't drown out useful test output.
_NOISY_LOGGERS = (
    "azure",
    "urllib3",
    "requests",
    "msal",
)

for _name in _NOISY_LOGGERS:
    logging.getLogger(_name).setLevel(logging.WARNING)


def _safe_test_name(item: pytest.Item) -> str:
    """Build a filesystem-safe log file name from a test item."""
    parts: list[str] = []
    if item.cls:
        parts.append(item.cls.__name__)
    parts.append(item.name)
    label = "__".join(parts)
    return label.replace("[", "_").replace("]", "").replace("/", "_").replace("\\", "_")


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_protocol(item: pytest.Item, nextitem: pytest.Item | None):
    """Wrap the entire test lifecycle with a per-test log file.

    Runs **before** any fixture setup (including session-scoped fixtures),
    so even fixture errors are captured in the per-test log.

    The root logger level is NOT changed — console output stays at whatever
    level pytest's ``log_cli_level`` is set to (INFO by default).
    """
    LOGS_DIR.mkdir(parents=True, exist_ok=True)

    safe_name = _safe_test_name(item)
    log_file = LOGS_DIR / f"{safe_name}.log"

    handler = logging.FileHandler(log_file, mode="w", encoding="utf-8")
    handler.setLevel(logging.DEBUG)
    handler.setFormatter(
        logging.Formatter(
            "%(asctime)s [%(levelname)s] %(name)s: %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
    )

    root_logger = logging.getLogger()
    root_logger.addHandler(handler)

    yield

    handler.flush()
    handler.close()
    root_logger.removeHandler(handler)
