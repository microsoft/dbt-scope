#!/usr/bin/env python3
"""IMDS Relay Router — proxies az login --identity token requests to Azure Relay."""

import base64
import hashlib
import hmac
import json
import logging
import os
import sys
import threading
import time
import urllib.parse
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

LISTEN_PORT = int(os.environ.get("IMDS_ROUTER_PORT", "8080"))
RELAY_URL = os.environ.get("IMDS_RELAY_URL", "")
RELAY_SENDER_KEY = os.environ.get("IMDS_RELAY_SENDER_KEY", "")
RELAY_KEY_NAME = os.environ.get("IMDS_RELAY_KEY_NAME", "Send")
IDENTITY_HEADER_VALUE = os.environ.get("IDENTITY_HEADER", "local-dev-secret")
TOKEN_MAX_ATTEMPTS = int(os.environ.get("IMDS_TOKEN_MAX_ATTEMPTS", "3"))
TOKEN_EXPIRY_SKEW_SEC = int(os.environ.get("IMDS_TOKEN_EXPIRY_SKEW_SEC", "300"))
LOG_DIR = os.environ.get("IMDS_LOG_DIR", ".logs")
LOG_FILE = os.path.join(LOG_DIR, "imds-relay-router.log")
STARTUP_PROBE_RESOURCE = os.environ.get(
    "IMDS_STARTUP_PROBE_RESOURCE", "https://management.azure.com/"
)
STARTUP_PROBE_CLIENT_ID = os.environ.get("UAMI_CLIENT_ID") or None


def _configure_logging() -> logging.Logger:
    os.makedirs(LOG_DIR, exist_ok=True)
    logger = logging.getLogger("imds-relay-router")
    logger.setLevel(logging.DEBUG)
    fmt = logging.Formatter("%(asctime)s [%(levelname)s] [imds-router] %(message)s")
    file_handler = logging.FileHandler(LOG_FILE)
    file_handler.setFormatter(fmt)
    stream_handler = logging.StreamHandler(sys.stdout)
    stream_handler.setFormatter(fmt)
    logger.handlers.clear()
    logger.addHandler(file_handler)
    logger.addHandler(stream_handler)
    logger.propagate = False
    return logger


log = _configure_logging()


def _relay_sas_uri(url: str) -> str:
    parsed = urllib.parse.urlparse(url)
    path = parsed.path.rstrip("/").removesuffix("/token")
    return f"http://{parsed.hostname}{path}"


def _generate_sas_token(uri: str, key: str, key_name: str, expiry_seconds: int = 3600) -> str:
    expiry = int(time.time()) + expiry_seconds
    sts = f"{urllib.parse.quote(uri, safe='')}\n{expiry}"
    sig = hmac.new(key.encode(), sts.encode(), hashlib.sha256).digest()
    sig_b64 = urllib.parse.quote(base64.b64encode(sig).decode(), safe="")
    return f"SharedAccessSignature sr={urllib.parse.quote(uri, safe='')}&sig={sig_b64}&se={expiry}&skn={key_name}"


def _fetch_token_from_relay(resource: str, client_id: str | None) -> tuple[dict | None, str]:
    """Fetch a token for *resource* from the relay, with bounded retries.

    Returns ``(body, "")`` on success or ``(None, error)`` on failure. Shared by
    the HTTP handler and the startup self-test so both exercise the same path.
    """
    relay_uri = f"{RELAY_URL}?resource={urllib.parse.quote_plus(resource)}"
    if client_id:
        relay_uri += f"&client_id={urllib.parse.quote_plus(client_id)}"

    last_error = "unknown error"
    for attempt in range(1, TOKEN_MAX_ATTEMPTS + 1):
        try:
            sas = _generate_sas_token(_relay_sas_uri(RELAY_URL), RELAY_SENDER_KEY, RELAY_KEY_NAME)
            req = urllib.request.Request(relay_uri, headers={"ServiceBusAuthorization": sas})
            with urllib.request.urlopen(req, timeout=30) as resp:
                body = json.loads(resp.read().decode())
            access_token = body.get("access_token", "")
            if access_token:
                log.info(
                    "Relay token acquired for resource=%s (attempt %d/%d, expires_on=%s)",
                    resource,
                    attempt,
                    TOKEN_MAX_ATTEMPTS,
                    body.get("expires_on"),
                )
                return body, ""
            last_error = "relay returned empty access_token"
        except urllib.error.HTTPError as e:
            detail = e.read().decode() if e.fp else ""
            last_error = f"Relay HTTP {e.code}: {detail[:200]}"
        except Exception as e:
            last_error = f"{type(e).__name__}: {e}"

        log.warning(
            "Relay token attempt %d/%d failed for resource=%s: %s",
            attempt,
            TOKEN_MAX_ATTEMPTS,
            resource,
            last_error,
        )
        if attempt < TOKEN_MAX_ATTEMPTS:
            time.sleep(min(2**attempt, 10))

    return None, last_error


class TokenCache:
    """Route-/scope-aware token cache.

    Keyed by resource (+ client_id), so different scopes never share an entry. A
    token within ``skew_sec`` of expiry is treated as stale so callers refetch
    ahead of expiry rather than serving a token about to expire. This collapses
    the many repeated ``az`` token requests (one per worker per scope) down to a
    single upstream relay fetch per scope, which is the dominant defense against
    intermittent relay failures.
    """

    def __init__(self, skew_sec, now=lambda: int(time.time())):
        self._entries = {}
        self._skew_sec = skew_sec
        self._now = now
        self._lock = threading.Lock()

    def is_fresh(self, entry):
        return entry["expires_on"] - self._now() > self._skew_sec

    def get(self, key):
        with self._lock:
            hit = self._entries.get(key)
            if hit is None:
                return None
            if self.is_fresh(hit):
                return hit
            del self._entries[key]
            return None

    def set(self, key, entry):
        with self._lock:
            self._entries[key] = entry


def _parse_expires_on(value):
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


_CACHE = TokenCache(TOKEN_EXPIRY_SKEW_SEC)


class Handler(BaseHTTPRequestHandler):
    def log_message(self, fmt, *args):
        log.debug(fmt % args)

    def do_GET(self):
        parsed = urllib.parse.urlparse(self.path)

        if parsed.path == "/healthz":
            return self._json(200, {"Healthy": True})
        if parsed.path in ("/metadata/identity/oauth2/token", "/token"):
            return self._handle_token(parsed)
        if parsed.path.startswith("/metadata/instance"):
            return self._json(200, {"compute": {"subscriptionId": os.environ.get("IMDS_SUBSCRIPTION_ID", "00000000-0000-0000-0000-000000000000"), "resourceGroupName": "github-actions", "name": "github-runner"}})
        log.warning("Unhandled request path: %s", parsed.path)
        self._json(404, {"error": "Not found"})

    def _handle_token(self, parsed):
        if self.headers.get("X-IDENTITY-HEADER", "") != IDENTITY_HEADER_VALUE:
            log.warning("Rejected token request with invalid X-IDENTITY-HEADER")
            return self._json(403, {"error": "Invalid X-IDENTITY-HEADER"})

        qs = urllib.parse.parse_qs(parsed.query)
        resource = qs.get("resource", ["https://management.azure.com/"])[0]
        client_id = qs.get("client_id", [None])[0]
        log.info("Token request: resource=%s client_id=%s", resource, client_id)

        if not RELAY_URL or not RELAY_SENDER_KEY:
            log.error(
                "Token request failed: relay not configured "
                "(IMDS_RELAY_URL set=%s, IMDS_RELAY_SENDER_KEY set=%s)",
                bool(RELAY_URL),
                bool(RELAY_SENDER_KEY),
            )
            return self._json(500, {"error": "IMDS_RELAY_URL or IMDS_RELAY_SENDER_KEY not set"})

        cache_key = resource if not client_id else f"{resource}|client_id={client_id}"
        cached = _CACHE.get(cache_key)
        if cached:
            log.info("Cache hit: %s (expires_on=%s)", cache_key, cached["expires_on"])
            return self._json(200, {"access_token": cached["access_token"], "expires_on": str(cached["expires_on"]), "resource": resource, "token_type": "Bearer"})

        body, error = _fetch_token_from_relay(resource, client_id)
        if body is not None:
            access_token = body.get("access_token", "")
            expires_on = _parse_expires_on(body.get("expires_on"))
            if expires_on is not None:
                _CACHE.set(cache_key, {"access_token": access_token, "expires_on": expires_on})
            return self._json(200, {"access_token": access_token, "expires_on": str(body.get("expires_on", "")), "resource": resource, "token_type": "Bearer"})

        log.error("Relay token request failed for resource=%s: %s", resource, error)
        self._json(502, {"error": "Relay token request failed", "detail": error[:500]})

    def _json(self, status: int, body: dict):
        payload = json.dumps(body).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)


def _validate_startup() -> None:
    """Fail fast unless the relay is configured, reachable, and issuing tokens.

    Raises ``SystemExit(1)`` (via ``sys.exit``) with a clear log message when the
    relay secrets are missing or the relay endpoint cannot mint a token, so CI
    surfaces the problem here instead of far downstream in ``az login``.
    """
    if not RELAY_URL or not RELAY_SENDER_KEY:
        log.error(
            "FATAL: relay secrets not set as expected "
            "(IMDS_RELAY_URL set=%s, IMDS_RELAY_SENDER_KEY set=%s). "
            "Refusing to start — pass IMDS_RELAY_URL and IMDS_RELAY_SENDER_KEY.",
            bool(RELAY_URL),
            bool(RELAY_SENDER_KEY),
        )
        sys.exit(1)

    log.info(
        "Startup self-test: probing relay for a token (resource=%s, client_id=%s)",
        STARTUP_PROBE_RESOURCE,
        STARTUP_PROBE_CLIENT_ID,
    )
    body, error = _fetch_token_from_relay(STARTUP_PROBE_RESOURCE, STARTUP_PROBE_CLIENT_ID)
    if body is None or not body.get("access_token"):
        log.error(
            "FATAL: relay endpoint not reachable or did not return a token: %s. "
            "Refusing to start.",
            error or "empty access_token",
        )
        sys.exit(1)

    log.info(
        "Startup self-test passed: relay reachable and issuing tokens "
        "(expires_on=%s).",
        body.get("expires_on"),
    )


if __name__ == "__main__":
    log.info(
        "Starting IMDS relay router on 0.0.0.0:%d (relay host=%s, log=%s)",
        LISTEN_PORT,
        urllib.parse.urlparse(RELAY_URL).hostname if RELAY_URL else None,
        LOG_FILE,
    )
    _validate_startup()
    server = ThreadingHTTPServer(("0.0.0.0", LISTEN_PORT), Handler)
    log.info("Listening on 0.0.0.0:%d", LISTEN_PORT)
    server.serve_forever()
