"""Lazy dotted-path loader for user-supplied ``TokenCredential`` implementations.

Mirrors the pattern from dbt-fabricspark PR #177 (``livysession._load_custom_credential``):
the user's profile carries a dotted path under ``credential_class`` and an
arbitrary ``credential_kwargs`` mapping. We:

1. Validate the dotted path against an identifier regex (defence-in-depth — ``importlib``
   wouldn't shell out, but rejecting non-identifier chars gives clearer errors).
2. ``importlib.import_module`` the module portion.
3. ``getattr`` the class portion and ``cls(**kwargs)`` it.
4. Enforce ``isinstance(instance, azure.core.credentials.TokenCredential)`` — the protocol
   is ``@runtime_checkable`` so this checks for a callable ``get_token``.

Instances are cached process-wide keyed by ``(dotted_path, repr-of-sorted-kwargs)`` so
that refreshes reuse the same object (matching how ``azure-identity`` credentials
are typically held) and so that the YAML round-trip of nested dicts/lists doesn't
defeat caching.
"""

from __future__ import annotations

import importlib
import re
import threading
from typing import Any

from azure.core.credentials import TokenCredential
from dbt_common.exceptions import DbtRuntimeError

_DOTTED_PATH_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*(\.[A-Za-z_][A-Za-z0-9_]*)+$")

_custom_credential_cache: dict[tuple[str, str], TokenCredential] = {}
_custom_credential_lock = threading.Lock()


def _cache_key(dotted: str, kwargs: dict[str, Any]) -> tuple[str, str]:
    # repr() of sorted items keeps the key hashable even when kwargs contain
    # nested dicts/lists from YAML.
    return (dotted, repr(sorted(kwargs.items(), key=lambda kv: kv[0])))


def load_custom_credential(dotted: str | None, kwargs: dict[str, Any] | None) -> TokenCredential:
    """Import and instantiate the user-supplied ``TokenCredential``."""
    if not dotted:
        raise DbtRuntimeError(
            "authentication='token_credential' requires `credential_class` "
            "(dotted path to an azure.core.credentials.TokenCredential)."
        )
    if not _DOTTED_PATH_PATTERN.match(dotted):
        raise DbtRuntimeError(
            f"credential_class must be a dotted path like 'pkg.module.ClassName', got: {dotted!r}"
        )
    kwargs = kwargs or {}
    key = _cache_key(dotted, kwargs)
    with _custom_credential_lock:
        cached = _custom_credential_cache.get(key)
        if cached is not None:
            return cached
        module_path, _, class_name = dotted.rpartition(".")
        try:
            module = importlib.import_module(module_path)
        except ImportError as exc:
            raise DbtRuntimeError(
                f"Could not import module for credential_class={dotted!r}: {exc}"
            ) from exc
        try:
            cls = getattr(module, class_name)
        except AttributeError as exc:
            raise DbtRuntimeError(
                f"Module {module_path!r} has no attribute {class_name!r} "
                f"(from credential_class={dotted!r})"
            ) from exc
        try:
            instance = cls(**kwargs)
        except TypeError as exc:
            raise DbtRuntimeError(
                f"Failed to instantiate {dotted!r} with credential_kwargs: {exc}"
            ) from exc
        # TokenCredential is @runtime_checkable — this checks for callable get_token.
        if not isinstance(instance, TokenCredential):
            raise DbtRuntimeError(
                f"{dotted!r} must implement azure.core.credentials.TokenCredential "
                f"(missing callable get_token)."
            )
        _custom_credential_cache[key] = instance
        return instance


def clear_cache() -> None:
    """Clear the process-wide cache. Intended for tests."""
    with _custom_credential_lock:
        _custom_credential_cache.clear()
