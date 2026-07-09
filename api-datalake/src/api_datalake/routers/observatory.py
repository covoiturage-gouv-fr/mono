"""Endpoints publics de l'observatoire."""

from fastapi import APIRouter, Depends, Query, Response

from ..cache import build_cache_key, get_redis, gzip_payload, publication_version
from ..config import settings
from ..db import connection
from ..helpers import check_territory_param
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


@router.get("/last-record")
async def last_record(
    code: str = Query(...),
    type: str = Query("com"),
    conn=Depends(get_conn),
):
    """Dernier mois disponible pour un territoire, borné par le cutoff de publication."""
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
    # Fenêtre de publication : période non publiée -> heatmap vide.
    if not is_published(settings.app_observatory_published_until, year, month, trimester, semester):
        return _gzip_json(gzip_payload([]), "BYPASS")

    cutoff = settings.app_observatory_published_until
    # `type` normalisé (allowlist) dans la clé pour éviter des entrées dupliquées
    # (type=com et type=inconnu -> même résultat via le fallback).
    params = {"code": code, "type": check_territory_param(type), "year": year,
              "month": month, "trimester": trimester, "semester": semester, "n": n}

    key = None
    if redis is not None:
        version = await publication_version(redis, cutoff)
        key = build_cache_key(version, "/observatory/location", params)
        hit = await redis.get(key)
        if hit is not None:
            return _gzip_json(hit, "HIT")

    data = await repo.get_location(conn, type, code, year, n, month, trimester, semester)
    blob = gzip_payload(data)
    if redis is not None and key is not None:
        await redis.set(key, blob, ex=settings.cache_ttl_seconds)
    return _gzip_json(blob, "MISS")
