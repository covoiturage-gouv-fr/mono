"""Endpoints publics de l'observatoire."""

from fastapi import APIRouter, Depends, Query, Response

from ..cache import (
    build_cache_key,
    cache_get,
    cache_set,
    get_redis,
    gzip_payload,
    publication_version,
)
from ..config import settings
from ..db import connection
from ..helpers import check_code_param, check_territory_param
from ..period import is_published, last_record_cutoff
from ..repositories import observatory as repo

router = APIRouter(prefix="/observatory", tags=["observatory"])


async def get_conn():
    async with connection() as conn:
        yield conn


def _gzip_json(blob: bytes, cache_state: str) -> Response:
    """Sert un payload déjà gzippé, avec les en-têtes de cache."""
    return Response(
        content=blob,
        media_type="application/json",
        headers={"Content-Encoding": "gzip", "X-Cache": cache_state},
    )


async def _serve_cached(redis, route: str, params: dict, produce) -> Response:
    """Sert `produce()` (données PG) avec cache Redis best-effort.

    Une panne Redis dégrade en cache-miss (`X-Cache: BYPASS`), jamais en 500.
    """
    cutoff = settings.app_observatory_published_until
    key = None
    if redis is not None:
        version = await publication_version(redis, cutoff)
        key = build_cache_key(version, route, params)

    hit, ok = await cache_get(redis, key)
    if hit is not None:
        return _gzip_json(hit, "HIT")

    blob = gzip_payload(await produce())
    if ok:
        await cache_set(redis, key, blob, settings.cache_ttl_seconds)
    return _gzip_json(blob, "MISS" if ok else "BYPASS")


@router.get("/last-record")
async def last_record(
    code: str = Query(...),
    type: str = Query("com"),
    conn=Depends(get_conn),
):
    """Dernier mois disponible pour un territoire, borné par le cutoff de publication."""
    check_code_param(code)
    cutoff = last_record_cutoff(settings.app_observatory_published_until)
    max_ym = cutoff[0] * 100 + cutoff[1] if cutoff else None
    return await repo.get_last_record(conn, type, code, max_ym)


@router.get("/location")
async def location(
    code: str = Query(...),
    type: str = Query("com"),
    year: int = Query(..., ge=2015, le=2100),
    month: int | None = Query(None, ge=1, le=12),
    trimester: int | None = Query(None, ge=1, le=4),
    semester: int | None = Query(None, ge=1, le=2),
    n: int = Query(5, ge=0, le=8),
    conn=Depends(get_conn),
    redis=Depends(get_redis),
):
    """Heatmap de densité (H3) d'un territoire. Binning en SQL, réponse gzip cachée."""
    check_code_param(code)
    # Fenêtre de publication : période non publiée -> heatmap vide.
    if not is_published(settings.app_observatory_published_until, year, month, trimester, semester):
        return _gzip_json(gzip_payload([]), "BYPASS")

    # `type` normalisé (allowlist) dans la clé pour éviter des entrées dupliquées
    # (type=com et type=inconnu -> même résultat via le fallback).
    params = {"code": code, "type": check_territory_param(type), "year": year,
              "month": month, "trimester": trimester, "semester": semester, "n": n}
    return await _serve_cached(
        redis, "/observatory/location", params,
        lambda: repo.get_location(conn, type, code, year, n, month, trimester, semester),
    )


@router.get("/campaigns")
async def campaigns(
    type: str | None = Query(None),
    code: str | None = Query(None),
    year: int | None = Query(None, ge=2015, le=2100),
    conn=Depends(get_conn),
    redis=Depends(get_redis),
):
    """Campagnes d'incitation (avec géométrie du territoire). Réponse gzip cachée."""
    if code is not None:
        check_code_param(code)
    params = {"type": check_territory_param(type) if type is not None else None,
              "code": code, "year": year}
    return await _serve_cached(
        redis, "/observatory/campaigns", params,
        lambda: repo.get_campaigns(conn, type, code, year),
    )
