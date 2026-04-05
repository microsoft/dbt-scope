"""ScopeConnectionManager — submit SCOPE scripts as ADLA jobs."""

from __future__ import annotations

import logging
import time
import uuid
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Any

import agate
import requests
from azure.identity import AzureCliCredential
from dbt.adapters.base import BaseConnectionManager
from dbt.adapters.contracts.connection import (
    AdapterResponse,
    Connection,
    ConnectionState,
)
from dbt_common.exceptions import DbtDatabaseError, DbtRuntimeError
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

from dbt.adapters.scope._file_lock import AZ_CLI_TOKEN_LOCK, FileLock
from dbt.adapters.scope.credentials import ScopeCredentials

log = logging.getLogger(__name__)

ADLA_TOKEN_SCOPE = "https://datalake.azure.net/.default"
API_VERSION = "2017-09-01-preview"

# Terminal ADLA job states
_TERMINAL_STATES = {"Ended"}
_SUCCESS_RESULTS = {"Succeeded"}


@dataclass
class ADLAJob:
    """Lightweight job tracker returned by ``submit_job``."""

    job_id: str
    name: str
    state: str = "Preparing"
    result: str | None = None
    error_message: str | None = None
    raw: dict[str, Any] = field(default_factory=dict, repr=False)

    @property
    def is_terminal(self) -> bool:
        return self.state in _TERMINAL_STATES

    @property
    def succeeded(self) -> bool:
        return self.state == "Ended" and self.result in _SUCCESS_RESULTS

    def update_from_response(self, data: dict[str, Any]) -> None:
        self.raw = data
        self.state = data.get("state", self.state)
        self.result = data.get("result", self.result)
        if self.state == "Ended" and self.result not in _SUCCESS_RESULTS:
            self.error_message = self._extract_error(data)

    @staticmethod
    def _extract_error(data: dict) -> str | None:
        # errorMessage can be at top level (list of error objects) or in properties
        err = data.get("errorMessage")
        if not err:
            err = data.get("properties", {}).get("errorMessage")
        if isinstance(err, list):
            msgs = [e.get("message", str(e)) for e in err if isinstance(e, dict)]
            return "; ".join(msgs) if msgs else str(err)
        return str(err) if err else None


class ScopeConnectionHandle:
    """Wraps an ADLA REST client with credential caching."""

    def __init__(self, credentials: ScopeCredentials) -> None:
        self._credentials = credentials
        self._account = credentials.adla_account
        self._base_url = f"https://{self._account}.azuredatalakeanalytics.net"
        self._timeout = credentials.http_timeout_seconds
        self._credential = AzureCliCredential()
        self._session = self._build_session(credentials.http_retries)
        self._cached_token: str | None = None
        self._token_expires_at: float = 0
        self._next_job_name: str | None = None

    # -- Job operations -----------------------------------------------

    def submit_job(self, name: str, script: str, au: int, priority: int) -> ADLAJob:
        job_id = str(uuid.uuid4())
        url = f"{self._base_url}/jobs/{job_id}?api-version={API_VERSION}"
        body = {
            "jobId": job_id,
            "name": name,
            "type": "Scope",
            "degreeOfParallelism": au,
            "priority": priority,
            "properties": {"type": "Scope", "script": script},
        }
        log.info("Submitting SCOPE job '%s' (AU=%d) → %s", name, au, job_id)
        resp = self._request("PUT", url, json=body)
        job = ADLAJob(job_id=job_id, name=name)
        job.update_from_response(resp)
        return job

    def poll_job(self, job: ADLAJob) -> ADLAJob:
        url = f"{self._base_url}/jobs/{job.job_id}?api-version={API_VERSION}"
        resp = self._request("GET", url)
        job.update_from_response(resp)
        return job

    def cancel_job(self, job_id: str) -> None:
        url = f"{self._base_url}/jobs/{job_id}/CancelJob?api-version={API_VERSION}"
        log.info("Cancelling ADLA job %s", job_id)
        self._request("POST", url)

    def submit_and_wait(
        self,
        name: str,
        script: str,
        au: int,
        priority: int,
        poll_interval: int = 5,
        max_wait: int = 3600,
    ) -> ADLAJob:
        """Submit a SCOPE job and poll until terminal."""
        job = self.submit_job(name, script, au, priority)
        start = time.monotonic()
        last_state = job.state

        while not job.is_terminal:
            elapsed = time.monotonic() - start
            if elapsed >= max_wait:
                raise DbtRuntimeError(
                    f"SCOPE job '{name}' ({job.job_id}) timed out after "
                    f"{elapsed:.0f}s in state {job.state}"
                )
            time.sleep(poll_interval)
            self.poll_job(job)
            if job.state != last_state:
                log.info("[%s] %s → %s", name, last_state, job.state)
                last_state = job.state

        if not job.succeeded:
            raise DbtDatabaseError(f"SCOPE job '{name}' ({job.job_id}) failed: {job.error_message}")

        log.info("[%s] Completed successfully (%s)", name, job.result)
        return job

    # -- Internal -----------------------------------------------------

    def _get_token(self) -> str:
        if self._cached_token and time.time() < self._token_expires_at - 300:
            return self._cached_token
        with FileLock(AZ_CLI_TOKEN_LOCK):
            token = self._credential.get_token(ADLA_TOKEN_SCOPE)
        self._cached_token = token.token
        self._token_expires_at = token.expires_on
        return self._cached_token

    def _request(self, method: str, url: str, **kwargs: Any) -> dict:
        headers = {
            "Authorization": f"Bearer {self._get_token()}",
            "Content-Type": "application/json",
            "Accept": "application/json",
        }
        resp = self._session.request(method, url, headers=headers, timeout=self._timeout, **kwargs)
        if resp.status_code >= 400:
            raise DbtDatabaseError(
                f"ADLA API {method} {url} returned {resp.status_code}: {resp.text[:500]}"
            )
        return resp.json()

    @staticmethod
    def _build_session(retries: int) -> requests.Session:
        session = requests.Session()
        retry = Retry(
            total=retries,
            backoff_factor=1,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET", "PUT", "POST"],
        )
        session.mount("https://", HTTPAdapter(max_retries=retry))
        return session


class ScopeConnectionManager(BaseConnectionManager):
    """Manages ADLA SCOPE connections for the dbt adapter."""

    TYPE = "scope"

    @classmethod
    def open(cls, connection: Connection) -> Connection:
        if connection.state == ConnectionState.OPEN:
            return connection

        credentials: ScopeCredentials = connection.credentials  # type: ignore[assignment]
        handle = ScopeConnectionHandle(credentials)
        connection.handle = handle
        connection.state = ConnectionState.OPEN
        return connection

    @classmethod
    def get_response(cls, _cursor: Any) -> AdapterResponse:
        return AdapterResponse(_message="OK")

    def cancel(self, connection: Connection) -> None:
        pass

    @classmethod
    def cancel_open(cls) -> None:
        pass

    @contextmanager
    def exception_handler(self, sql: str):  # type: ignore[override]
        try:
            yield
        except DbtDatabaseError:
            raise
        except Exception as exc:
            raise DbtDatabaseError(str(exc)) from exc

    def begin(self) -> None:
        pass

    def commit(self) -> None:
        pass

    def clear_transaction(self) -> None:
        pass

    def execute(
        self,
        sql: str,
        auto_begin: bool = False,
        fetch: bool = False,
        limit: int | None = None,
        *,
        job_name: str = "dbt-scope",
    ) -> tuple[AdapterResponse, agate.Table]:
        """Submit a SCOPE script as an ADLA job and wait for completion."""
        connection = self.get_thread_connection()
        handle: ScopeConnectionHandle = connection.handle  # type: ignore[assignment]
        credentials: ScopeCredentials = connection.credentials  # type: ignore[assignment]

        # Skip empty or comment-only scripts
        stripped = sql.strip()
        if not stripped or stripped.startswith("--"):
            return AdapterResponse(_message="SKIP"), agate.Table(rows=[])

        # Handle debug/test queries — verify token instead of submitting a job
        stripped_lower = stripped.lower().rstrip(";").strip()
        if stripped_lower in ("select 1", "select 1 as id"):
            handle._get_token()  # Verify Azure CLI auth works
            return AdapterResponse(_message="OK"), agate.Table(rows=[])

        with self.exception_handler(sql):
            effective_name = handle._next_job_name or job_name
            handle._next_job_name = None
            job = handle.submit_and_wait(
                name=effective_name,
                script=sql,
                au=credentials.au,
                priority=credentials.priority,
                poll_interval=credentials.poll_interval_seconds,
                max_wait=credentials.max_wait_seconds,
            )

        response = AdapterResponse(
            _message=f"OK job_id={job.job_id}",
            code=job.result or "OK",
        )
        return response, agate.Table(rows=[])
