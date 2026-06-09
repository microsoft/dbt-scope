"""Tests for custom_credential — dotted-path TokenCredential loader."""

from __future__ import annotations

import sys
import types
from datetime import datetime, timezone

import pytest
from azure.core.credentials import AccessToken, TokenCredential
from dbt_common.exceptions import DbtRuntimeError

from dbt.adapters.scope.custom_credential import (
    _cache_key,
    clear_cache,
    load_custom_credential,
)


class _FakeCredential:
    def __init__(self, **kwargs):
        self.kwargs = kwargs

    def get_token(self, *scopes, **kw):
        return AccessToken(
            token="fake-token", expires_on=int(datetime.now(tz=timezone.utc).timestamp()) + 3600
        )


class _StrictCredential:
    def __init__(self, foo: str):
        self.foo = foo

    def get_token(self, *scopes, **kw):
        return AccessToken(
            token="strict", expires_on=int(datetime.now(tz=timezone.utc).timestamp()) + 3600
        )


class _NotACredential:
    def __init__(self, **kwargs):
        pass


@pytest.fixture(autouse=True)
def _clear_cache():
    clear_cache()
    yield
    clear_cache()


@pytest.fixture
def fake_module():
    name = "_test_custom_cred_module"
    mod = types.ModuleType(name)
    mod.FakeCredential = _FakeCredential
    mod.StrictCredential = _StrictCredential
    mod.NotACredential = _NotACredential
    sys.modules[name] = mod
    yield name
    sys.modules.pop(name, None)


class TestLoadCustomCredential:
    def test_loads_and_isinstance(self, fake_module):
        cred = load_custom_credential(f"{fake_module}.FakeCredential", {"foo": "bar"})
        assert isinstance(cred, TokenCredential)
        assert cred.kwargs == {"foo": "bar"}

    def test_caches_instance(self, fake_module):
        first = load_custom_credential(f"{fake_module}.FakeCredential", {"foo": "bar"})
        second = load_custom_credential(f"{fake_module}.FakeCredential", {"foo": "bar"})
        assert first is second

    def test_cache_distinguishes_kwargs(self, fake_module):
        first = load_custom_credential(f"{fake_module}.FakeCredential", {"foo": "bar"})
        second = load_custom_credential(f"{fake_module}.FakeCredential", {"foo": "baz"})
        assert first is not second

    def test_cache_handles_nested_dicts(self, fake_module):
        nested = {"auth": {"method": "SNI", "sni": {"client_id": "abc"}}}
        first = load_custom_credential(f"{fake_module}.FakeCredential", nested)
        second = load_custom_credential(f"{fake_module}.FakeCredential", dict(nested))
        assert first is second

    def test_rejects_missing_class(self):
        with pytest.raises(DbtRuntimeError, match="requires `credential_class`"):
            load_custom_credential(None, {})
        with pytest.raises(DbtRuntimeError, match="requires `credential_class`"):
            load_custom_credential("", {})

    def test_rejects_non_dotted_path(self):
        with pytest.raises(DbtRuntimeError, match="dotted path"):
            load_custom_credential("notdotted", {})

    def test_rejects_invalid_identifier(self):
        with pytest.raises(DbtRuntimeError, match="dotted path"):
            load_custom_credential("pkg.123bad", {})

    def test_import_error_surfaces(self):
        with pytest.raises(DbtRuntimeError, match="Could not import module"):
            load_custom_credential("nonexistent_module_xyz.SomeClass", {})

    def test_attribute_error_surfaces(self, fake_module):
        with pytest.raises(DbtRuntimeError, match="no attribute"):
            load_custom_credential(f"{fake_module}.MissingClass", {})

    def test_type_error_surfaces(self, fake_module):
        with pytest.raises(DbtRuntimeError, match="Failed to instantiate"):
            load_custom_credential(f"{fake_module}.StrictCredential", {"unknown_kwarg": "value"})

    def test_isinstance_enforced(self, fake_module):
        with pytest.raises(DbtRuntimeError, match="must implement"):
            load_custom_credential(f"{fake_module}.NotACredential", {})


class TestCacheKey:
    def test_stable_for_reordered_kwargs(self):
        a = _cache_key("pkg.Cls", {"b": 1, "a": 2})
        b = _cache_key("pkg.Cls", {"a": 2, "b": 1})
        assert a == b

    def test_distinguishes_class(self):
        a = _cache_key("pkg.A", {"x": 1})
        b = _cache_key("pkg.B", {"x": 1})
        assert a != b
