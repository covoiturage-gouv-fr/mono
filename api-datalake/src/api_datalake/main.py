"""Application FastAPI `api-datalake` : API de lecture de l'observatoire public."""

from contextlib import asynccontextmanager

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from .cache import close_redis, open_redis
from .config import settings
from .db import close_pool, open_pool
from .routers import observatory


@asynccontextmanager
async def lifespan(app: FastAPI):
    pool = open_pool()
    await pool.open()
    open_redis()
    yield
    await close_redis()
    await close_pool()


def create_app() -> FastAPI:
    app = FastAPI(title="api-datalake", lifespan=lifespan)
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
