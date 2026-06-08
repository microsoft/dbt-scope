"""Unit tests for ``LockedTokenCredential`` + ``RetryPolicy``.

Verifies the credential-retry resilience added for the
``CredentialUnavailableError: Failed to invoke the Azure CLI`` failure
mode observed in production (PR #32).
"""

from __future__ import annotations

import tempfile
from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest
from azure.identity import CredentialUnavailableError

from dbt.adapters.scope.delta_lake import LockedTokenCredential, RetryPolicy


@pytest.fixture
def lock_path() -> str:
    """Per-test lock file under tmp to avoid cross-test contention."""
    with tempfile.NamedTemporaryFile(suffix=".lock", delete=False) as f:
        return f.name


class _RecordingSleep:
    """Record every sleep duration without actually sleeping."""

    def __init__(self) -> None:
        self.calls: list[float] = []

    def __call__(self, seconds: float) -> None:
        self.calls.append(seconds)


# -- RetryPolicy ---------------------------------------------------------


class TestRetryPolicy:
    def test_defaults(self) -> None:
        policy = RetryPolicy()
        assert policy.max_retries == 10
        assert policy.initial_delay_seconds == 1.0
        assert policy.max_delay_seconds == 10.0

    def test_from_http_retries_none_uses_defaults(self) -> None:
        policy = RetryPolicy.from_http_retries(None)
        assert policy.max_retries == 10

    def test_from_http_retries_negative_uses_defaults(self) -> None:
        policy = RetryPolicy.from_http_retries(-1)
        assert policy.max_retries == 10

    def test_from_http_retries_zero_disables_retries(self) -> None:
        # ``0`` means "do not retry" — only the initial attempt runs.
        policy = RetryPolicy.from_http_retries(0)
        assert policy.max_retries == 0

    def test_from_http_retries_passthrough(self) -> None:
        policy = RetryPolicy.from_http_retries(25)
        assert policy.max_retries == 25


# -- LockedTokenCredential.get_token -------------------------------------


class TestLockedTokenCredential:
    def test_succeeds_on_first_attempt_no_sleep(self, lock_path: str) -> None:
        inner = MagicMock()
        inner.get_token.return_value = SimpleNamespace(token="t", expires_on=0)
        sleep = _RecordingSleep()

        cred = LockedTokenCredential(
            inner,
            lock_file=lock_path,
            retry_policy=RetryPolicy(max_retries=3),
            sleep=sleep,
        )

        token = cred.get_token("https://example.com/.default")

        assert token.token == "t"
        assert inner.get_token.call_count == 1
        assert sleep.calls == []

    def test_succeeds_after_transient_failures(self, lock_path: str) -> None:
        inner = MagicMock()
        inner.get_token.side_effect = [
            CredentialUnavailableError(message="cli timeout 1"),
            CredentialUnavailableError(message="cli timeout 2"),
            SimpleNamespace(token="t", expires_on=0),
        ]
        sleep = _RecordingSleep()

        cred = LockedTokenCredential(
            inner,
            lock_file=lock_path,
            retry_policy=RetryPolicy(
                max_retries=5, initial_delay_seconds=1.0, max_delay_seconds=10.0
            ),
            sleep=sleep,
        )

        token = cred.get_token("scope")

        assert token.token == "t"
        assert inner.get_token.call_count == 3
        # Slept twice (after attempts 1 and 2), not after the success.
        assert sleep.calls == [1.0, 2.0]

    def test_exhausts_retries_and_reraises(self, lock_path: str) -> None:
        inner = MagicMock()
        inner.get_token.side_effect = CredentialUnavailableError(message="permanent")
        sleep = _RecordingSleep()

        cred = LockedTokenCredential(
            inner,
            lock_file=lock_path,
            retry_policy=RetryPolicy(
                max_retries=3, initial_delay_seconds=1.0, max_delay_seconds=10.0
            ),
            sleep=sleep,
        )

        with pytest.raises(CredentialUnavailableError):
            cred.get_token("scope")

        # 3 retries + 1 initial attempt == 4 total calls
        assert inner.get_token.call_count == 4
        # Sleeps happen between attempts: 1s, 2s, 3s (3 sleeps == max_retries)
        # No sleep after the final failed attempt.
        assert sleep.calls == [1.0, 2.0, 3.0]

    def test_linear_backoff_caps_at_max_delay(self, lock_path: str) -> None:
        inner = MagicMock()
        inner.get_token.side_effect = CredentialUnavailableError(message="x")
        sleep = _RecordingSleep()

        cred = LockedTokenCredential(
            inner,
            lock_file=lock_path,
            retry_policy=RetryPolicy(
                max_retries=15, initial_delay_seconds=1.0, max_delay_seconds=10.0
            ),
            sleep=sleep,
        )

        with pytest.raises(CredentialUnavailableError):
            cred.get_token("scope")

        # Linear ramp 1..10s then capped at 10s.
        expected = [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0, 10.0, 10.0, 10.0, 10.0, 10.0]
        assert sleep.calls == expected
        # 15 retries + 1 initial == 16 total calls; 15 sleeps (one per retry)
        assert inner.get_token.call_count == 16
        assert len(sleep.calls) == 15

    def test_zero_retries_makes_single_attempt(self, lock_path: str) -> None:
        inner = MagicMock()
        inner.get_token.side_effect = CredentialUnavailableError(message="x")
        sleep = _RecordingSleep()

        cred = LockedTokenCredential(
            inner,
            lock_file=lock_path,
            retry_policy=RetryPolicy(max_retries=0),
            sleep=sleep,
        )

        with pytest.raises(CredentialUnavailableError):
            cred.get_token("scope")

        assert inner.get_token.call_count == 1
        assert sleep.calls == []  # never sleep when no retries configured

    def test_does_not_retry_unrelated_exceptions(self, lock_path: str) -> None:
        inner = MagicMock()
        inner.get_token.side_effect = RuntimeError("boom")
        sleep = _RecordingSleep()

        cred = LockedTokenCredential(
            inner,
            lock_file=lock_path,
            retry_policy=RetryPolicy(max_retries=5),
            sleep=sleep,
        )

        with pytest.raises(RuntimeError, match="boom"):
            cred.get_token("scope")

        # Did NOT retry — only ``CredentialUnavailableError`` is retried.
        assert inner.get_token.call_count == 1
        assert sleep.calls == []

    def test_passes_claims_kwarg_through(self, lock_path: str) -> None:
        inner = MagicMock()
        inner.get_token.return_value = SimpleNamespace(token="t", expires_on=0)

        cred = LockedTokenCredential(inner, lock_file=lock_path)

        cred.get_token("scope", claims="my-claims")

        inner.get_token.assert_called_once_with("scope", claims="my-claims")

    def test_lock_is_released_between_attempts(self, lock_path: str) -> None:
        # The lock must be released between attempts so concurrent
        # workers can make progress. We verify by acquiring it from a
        # parallel "thread" (via the lock_file path on disk) in between
        # the failing attempts.
        from dbt.adapters.scope._file_lock import FileLock

        attempts_observed_unlocked: list[bool] = []
        call_counter = {"n": 0}

        def fake_get_token(*args, **kwargs):
            call_counter["n"] += 1
            if call_counter["n"] < 3:
                raise CredentialUnavailableError(message="transient")
            return SimpleNamespace(token="t", expires_on=0)

        inner = MagicMock()
        inner.get_token.side_effect = fake_get_token

        def sleep_and_probe(_seconds: float) -> None:
            # Between attempts, attempt to acquire the lock — should
            # succeed instantly because LockedTokenCredential released
            # it after the failed attempt.
            try:
                with FileLock(lock_path):
                    attempts_observed_unlocked.append(True)
            except Exception:
                attempts_observed_unlocked.append(False)

        cred = LockedTokenCredential(
            inner,
            lock_file=lock_path,
            retry_policy=RetryPolicy(max_retries=5),
            sleep=sleep_and_probe,
        )

        cred.get_token("scope")

        # We made 2 failed attempts → 2 sleeps → 2 probes.
        assert attempts_observed_unlocked == [True, True]

    def test_uses_default_policy_when_none(self, lock_path: str) -> None:
        # No explicit policy → default 10 retries.
        inner = MagicMock()
        inner.get_token.side_effect = CredentialUnavailableError(message="x")
        sleep = _RecordingSleep()

        cred = LockedTokenCredential(inner, lock_file=lock_path, sleep=sleep)

        with pytest.raises(CredentialUnavailableError):
            cred.get_token("scope")

        # Default RetryPolicy → 10 retries + 1 initial == 11 calls
        assert inner.get_token.call_count == 11
        assert len(sleep.calls) == 10


# -- build_credential lock-file dispatch ---------------------------------


class TestBuildCredentialLockFile:
    """``build_credential`` picks the lock file based on ``authentication``."""

    def test_cli_auth_uses_az_cli_lock(self) -> None:
        from dbt.adapters.scope._file_lock import AZ_CLI_TOKEN_LOCK
        from dbt.adapters.scope.delta_lake import build_credential

        creds = SimpleNamespace(authentication="cli", http_retries=0)
        cred = build_credential(creds)
        assert isinstance(cred, LockedTokenCredential)
        assert cred._lock_file == AZ_CLI_TOKEN_LOCK

    def test_token_credential_auth_uses_fabric_lock(self, monkeypatch: pytest.MonkeyPatch) -> None:
        from dbt.adapters.scope._file_lock import FABRIC_TOKEN_LOCK
        from dbt.adapters.scope.delta_lake import build_credential

        fake_inner = MagicMock(name="custom_inner")
        monkeypatch.setattr(
            "dbt.adapters.scope.custom_credential.load_custom_credential",
            lambda *_a, **_k: fake_inner,
        )
        creds = SimpleNamespace(
            authentication="token_credential",
            credential_class="some.module.SomeCred",
            credential_kwargs={},
            http_retries=0,
        )
        cred = build_credential(creds)
        assert isinstance(cred, LockedTokenCredential)
        assert cred._lock_file == FABRIC_TOKEN_LOCK
