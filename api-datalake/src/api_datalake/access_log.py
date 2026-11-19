"""Journal d'accès : une ligne JSON par requête sur stdout (hors sondes k8s réussies)."""

import contextlib
import json
import logging
import re
import sys
import time
from datetime import UTC, datetime

PROBE_PATHS = {"/health", "/health/ready"}
MAX_HEADER_LEN = 64

_UNSAFE = re.compile(r"[^A-Za-z0-9._-]")

# Logger dédié avec son propre handler : la racine n'en a pas sous uvicorn (info perdu).
access_logger = logging.getLogger("api_datalake.access")
access_logger.setLevel(logging.INFO)
access_logger.propagate = False
access_logger.addHandler(logging.StreamHandler(sys.stdout))


def clean_header(value: str | None) -> str | None:
    """Valeur d'en-tête contrôlée par le client : jeu de caractères et longueur bornés."""
    return _UNSAFE.sub("", value or "")[:MAX_HEADER_LEN] or None


class AccessLogMiddleware:
    """ASGI pur (pas BaseHTTPMiddleware) : voit le statut réel et les exceptions non gérées."""

    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            return await self.app(scope, receive, send)

        start = time.perf_counter()
        status = 500  # exception non gérée : la réponse 500 est émise au-dessus de nous

        async def send_wrapper(message):
            nonlocal status
            if message["type"] == "http.response.start":
                status = message["status"]
            await send(message)

        try:
            await self.app(scope, receive, send_wrapper)
        finally:
            if scope["path"] not in PROBE_PATHS or status >= 400:
                # Le journal d'accès ne doit jamais faire échouer une requête ni masquer son exception.
                with contextlib.suppress(Exception):
                    access_logger.info(json.dumps(_entry(scope, status, start), default=str))


def _entry(scope, status, start) -> dict:
    headers = {k: v.decode("latin1") for k, v in scope["headers"]}
    # Gabarit de la route, jamais le chemin brut ; absent sans correspondance (404, maintenance).
    route = scope.get("route")
    client = scope.get("client")
    state = scope.get("state") or {}
    entry = {
        "ts": datetime.now(UTC).isoformat(timespec="milliseconds"),
        "level": "info",
        "ip": client[0] if client else None,
        "request_id": clean_header(headers.get(b"x-request-id")),
        "method": scope["method"],
        "route": route.path if route else None,
        "status": status,
        "duration_ms": round((time.perf_counter() - start) * 1000),
        # Posés sur `request.state` par l'authentification, après validation du jeton.
        "sub": state.get("sub"),
        "role": state.get("role"),
        "pentest_id": clean_header(headers.get(b"x-pentest-id")),
    }
    return {k: v for k, v in entry.items() if v is not None}
