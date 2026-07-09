"""Pool de connexions PostgreSQL (lecture seule sur la base datalake)."""

from collections.abc import AsyncIterator
from contextlib import asynccontextmanager

from psycopg.rows import dict_row
from psycopg_pool import AsyncConnectionPool

from .config import settings

_pool: AsyncConnectionPool | None = None


def open_pool() -> AsyncConnectionPool:
    global _pool
    if _pool is None:
        _pool = AsyncConnectionPool(
            settings.conninfo(),
            min_size=1,
            max_size=8,
            kwargs={"row_factory": dict_row, "autocommit": True},
            open=False,
        )
    return _pool


async def close_pool() -> None:
    global _pool
    if _pool is not None:
        await _pool.close()
        _pool = None


@asynccontextmanager
async def connection() -> AsyncIterator:
    """FastAPI dependency : fournit une connexion du pool. Surchargeable en test."""
    pool = open_pool()
    async with pool.connection() as conn:
        yield conn
