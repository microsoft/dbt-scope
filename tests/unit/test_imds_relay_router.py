"""Unit tests for the CI IMDS relay router's token cache + helpers.

The router lives at ``.github/scripts/imds_relay_router.py`` (CI infra, not a
package), so it is loaded by path. Importing is side-effect free — the server
only starts under ``if __name__ == "__main__"``.
"""

from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest

_ROUTER_PATH = Path(__file__).resolve().parents[2] / ".github" / "scripts" / "imds_relay_router.py"


def _load_router():
    spec = importlib.util.spec_from_file_location("imds_relay_router", _ROUTER_PATH)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    return module


router = _load_router()


class TestParseExpiresOn:
    def test_parses_int_string(self):
        assert router._parse_expires_on("1700000000") == 1700000000

    def test_parses_int(self):
        assert router._parse_expires_on(1700000000) == 1700000000

    def test_returns_none_for_garbage(self):
        assert router._parse_expires_on("not-a-number") is None
        assert router._parse_expires_on(None) is None
        assert router._parse_expires_on("") is None


class TestTokenCache:
    def _cache(self, skew=300, start=1_000):
        clock = {"t": start}
        cache = router.TokenCache(skew_sec=skew, now=lambda: clock["t"])
        return cache, clock

    def test_miss_returns_none(self):
        cache, _ = self._cache()
        assert cache.get("storage") is None

    def test_hit_when_fresh(self):
        cache, clock = self._cache(skew=300)
        cache.set("storage", {"access_token": "tok", "expires_on": clock["t"] + 3600})
        hit = cache.get("storage")
        assert hit is not None
        assert hit["access_token"] == "tok"

    def test_stale_within_skew_is_evicted(self):
        cache, clock = self._cache(skew=300)
        # Expires in 200s, which is inside the 300s skew → treated as stale.
        cache.set("storage", {"access_token": "tok", "expires_on": clock["t"] + 200})
        assert cache.get("storage") is None
        # Eviction is persistent (entry removed, not just hidden).
        assert cache.get("storage") is None

    def test_expired_is_evicted(self):
        cache, clock = self._cache(skew=300)
        cache.set("storage", {"access_token": "tok", "expires_on": clock["t"] + 3600})
        assert cache.get("storage") is not None
        clock["t"] += 3600  # now past expiry
        assert cache.get("storage") is None

    def test_keys_are_isolated(self):
        cache, clock = self._cache()
        cache.set("storage", {"access_token": "s", "expires_on": clock["t"] + 3600})
        cache.set("datalake", {"access_token": "d", "expires_on": clock["t"] + 3600})
        assert cache.get("storage")["access_token"] == "s"
        assert cache.get("datalake")["access_token"] == "d"

    @pytest.mark.parametrize(
        ("expires_in", "skew", "fresh"),
        [(3600, 300, True), (301, 300, True), (300, 300, False), (0, 300, False)],
    )
    def test_is_fresh_boundary(self, expires_in, skew, fresh):
        cache, clock = self._cache(skew=skew)
        entry = {"access_token": "t", "expires_on": clock["t"] + expires_in}
        assert cache.is_fresh(entry) is fresh
