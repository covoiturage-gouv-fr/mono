"""Application FastAPI `api-datalake` : API de lecture de l'observatoire public."""

import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from starlette.middleware.base import BaseHTTPMiddleware
from starlette.responses import JSONResponse

from .cache import close_redis, open_redis
from .config import settings
from .db import close_pool, open_pool
from .routers import observatory

logger = logging.getLogger("api_datalake")

# Routes toujours servies, y compris en maintenance (sondes k8s liveness/readiness).
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
    app = FastAPI(title="api-datalake", lifespan=lifespan)
    app.state.settings = settings  # source de vérité unique, surchargeable en test
    app.add_middleware(MaintenanceMiddleware)
    app.add_middleware(
        CORSMiddleware,
        allow_origins=[o.strip() for o in settings.cors_origins.split(",")],
        allow_methods=["GET"],
        allow_headers=["*"],
    )

    @app.get("/health")
    async def health():
        return {"status": "ok"}

    app.include_router(observatory.router)
    return app


app = create_app()
