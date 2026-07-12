"""Application FastAPI `api-datalake` : API de lecture de l'observatoire public."""

import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.exceptions import RequestValidationError
from fastapi.middleware.cors import CORSMiddleware
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import JSONResponse

from .cache import close_redis, open_redis
from .config import settings
from .db import close_pool, open_pool
from .routers import observatory

logger = logging.getLogger("api_datalake")

# Version de contrat de l'API publique : le préfixe d'URL EST la version. Les
# consommateurs (app-observatoire, réutilisateurs open-data) épinglent `/v3`. Un
# changement cassant se fait en ajoutant `/v4` — on ne casse jamais `/v3` en place.
API_VERSION = "v3"

# Routes toujours servies, y compris en maintenance (sondes k8s liveness/readiness,
# non versionnées : ce sont des sondes d'ops, pas du contrat public).
HEALTH_PATHS = {"/health"}


class MaintenanceMiddleware(BaseHTTPMiddleware):
    """En maintenance : 503 sur tout, sauf `/health`. Court-circuite avant PG/Redis."""

    async def dispatch(self, request, call_next):
        if request.app.state.settings.maintenance_mode and request.url.path not in HEALTH_PATHS:
            return JSONResponse(
                {"status": "maintenance"},
                status_code=503,
                headers={"Retry-After": "3600"},
            )
        return await call_next(request)


@asynccontextmanager
async def lifespan(app: FastAPI):
    on = app.state.settings.maintenance_mode
    logger.info("maintenance mode: %s", "on" if on else "off")
    # En maintenance : aucune connexion PG/Redis (bascule pendant migration/backfill).
    if not on:
        pool = open_pool()
        await pool.open()
        open_redis()
    yield
    await close_redis()
    await close_pool()


def create_app() -> FastAPI:
    # openapi_url=None coupe /openapi.json ET, par voie de conséquence, /docs et /redoc
    # (les UI dérivent du schéma) : on n'expose pas le schéma sur une API publique.
    app = FastAPI(title="api-datalake", lifespan=lifespan, openapi_url=None)
    app.state.settings = settings  # source de vérité unique, surchargeable en test

    @app.exception_handler(RequestValidationError)
    async def _validation_handler(request, exc):
        # Réponse laconique : ne pas renvoyer la valeur invalide ni la structure du champ.
        # Trace d'observabilité sans la valeur brute (détection de scan/fuzzing).
        logger.info("validation error on %s %s", request.method, request.url.path)
        return JSONResponse({"detail": "invalid request parameters"}, status_code=422)

    app.add_middleware(MaintenanceMiddleware)
    app.add_middleware(
        CORSMiddleware,
        allow_origins=[o.strip() for o in settings.cors_origins.split(",")],
        allow_methods=["GET"],
        allow_headers=["*"],
    )

    @app.get("/health")
    async def health():
        """Liveness : le process répond. Toujours 200 (y compris en maintenance)."""
        return {"status": "ok"}

    @app.get("/health/ready")
    async def ready():
        """Readiness : le pool PG répond (SELECT 1). 503 si la base est injoignable,
        pour que k8s retire le pod du service. En maintenance, le middleware 503 avant."""
        try:
            async with open_pool().connection() as conn:
                async with conn.cursor() as cur:
                    await cur.execute("SELECT 1")
                    await cur.fetchone()
        except Exception:
            logger.exception("readiness check failed")
            return JSONResponse({"status": "unavailable"}, status_code=503)
        return {"status": "ready"}

    # Monté sous /v3 : les routes publiques sont /v3/observatory/... (contrat préservé).
    app.include_router(observatory.router, prefix=f"/{API_VERSION}")
    return app


app = create_app()
