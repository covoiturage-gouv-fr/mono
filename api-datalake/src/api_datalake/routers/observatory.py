"""Endpoints publics de l'observatoire."""

from typing import Literal

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
from ..repositories import observatory_aggregated as agg

router = APIRouter(prefix="/observatory", tags=["observatory"])

Direction = Literal["from", "to", "both"]


def get_conn():
    """Fournit l'« acquéreur » de connexion (la fabrique `connection`), pas une
    connexion ouverte.

    Sur cache HIT, `acquire()` n'est jamais appelé : aucune connexion du pool n'est
    mobilisée. Sur MISS, la connexion n'est ouverte que le temps de la requête SQL,
    puis relâchée avant le gzip. Surchargeable en test.
    """
    return connection


def _gzip_json(blob: bytes, cache_state: str) -> Response:
    """Sert un payload déjà gzippé, avec les en-têtes de cache."""
    return Response(
        content=blob,
        media_type="application/json",
        headers={"Content-Encoding": "gzip", "X-Cache": cache_state},
    )


async def _serve_cached(redis, route: str, params: dict, produce, acquire) -> Response:
    """Sert `produce(conn)` (données PG) avec cache Redis best-effort.

    La connexion n'est ouverte (`acquire()`) que sur cache MISS, et relâchée dès la
    requête terminée — avant la sérialisation/gzip. Une panne Redis dégrade en
    cache-miss (`X-Cache: BYPASS`), jamais en 500.
    """
    cutoff = settings.app_observatory_published_until
    key = None
    if redis is not None:
        version = await publication_version(redis, cutoff)
        key = build_cache_key(version, route, params)

    hit, ok = await cache_get(redis, key)
    if hit is not None:
        return _gzip_json(hit, "HIT")

    async with acquire() as conn:
        data = await produce(conn)
    blob = gzip_payload(data)
    if ok:
        await cache_set(redis, key, blob, settings.cache_ttl_seconds)
    return _gzip_json(blob, "MISS" if ok else "BYPASS")


@router.get("/last-record")
async def last_record(
    code: str = Query(...),
    type: str = Query("com"),
    acquire=Depends(get_conn),
):
    """Dernier mois disponible pour un territoire, borné par le cutoff de publication."""
    check_code_param(code)
    cutoff = last_record_cutoff(settings.app_observatory_published_until)
    max_ym = cutoff[0] * 100 + cutoff[1] if cutoff else None
    async with acquire() as conn:
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
    acquire=Depends(get_conn),
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
        lambda conn: repo.get_location(conn, type, code, year, n, month, trimester, semester),
        acquire,
    )


@router.get("/campaigns")
async def campaigns(
    type: str | None = Query(None),
    code: str | None = Query(None),
    year: int | None = Query(None, ge=2015, le=2100),
    acquire=Depends(get_conn),
    redis=Depends(get_redis),
):
    """Campagnes d'incitation (avec géométrie du territoire). Réponse gzip cachée."""
    if code is not None:
        check_code_param(code)
    params = {"type": check_territory_param(type) if type is not None else None,
              "code": code, "year": year}
    return await _serve_cached(
        redis, "/observatory/campaigns", params,
        lambda conn: repo.get_campaigns(conn, type, code, year),
        acquire,
    )


# --------------------------------------------------------------------------- #
# Endpoints agrégés (flux, occupation, distribution, incentive, keyfigures, infra)
# Tous : GET public, réponse gzip cachée. Détail des requêtes dans
# repositories/observatory_aggregated.py.
# --------------------------------------------------------------------------- #


def _serve_rows(redis, route: str, params: dict, sql: str, sql_params: dict, acquire):
    return _serve_cached(redis, route, params,
                         lambda conn: agg.fetch(conn, sql, sql_params), acquire)


@router.get("/flux")
async def flux(
    code: str = Query(..., min_length=1, max_length=15),
    type: str = Query(...),
    observe: str = Query(...),
    year: int = Query(..., ge=2020, le=2100),
    month: int | None = Query(None, ge=1, le=12),
    trimester: int | None = Query(None, ge=1, le=4),
    semester: int | None = Query(None, ge=1, le=2),
    acquire=Depends(get_conn),
    redis=Depends(get_redis),
):
    """Flux OD entre territoires."""
    check_code_param(code)
    sql, sp = agg.build_flux(type, observe, code, year, month, trimester, semester)
    params = {"code": code, "type": check_territory_param(type),
              "observe": check_territory_param(observe), "year": year,
              "month": month, "trimester": trimester, "semester": semester}
    return await _serve_rows(redis, "/observatory/flux", params, sql, sp, acquire)


@router.get("/best-flux")
async def best_flux(
    code: str = Query(..., min_length=1, max_length=15),
    type: str = Query(...),
    year: int = Query(..., ge=2020, le=2100),
    limit: int = Query(10, ge=5, le=100),
    month: int | None = Query(None, ge=1, le=12),
    trimester: int | None = Query(None, ge=1, le=4),
    semester: int | None = Query(None, ge=1, le=2),
    acquire=Depends(get_conn),
    redis=Depends(get_redis),
):
    """Meilleurs flux d'un territoire (top N par trajets)."""
    check_code_param(code)
    sql, sp = agg.build_best_flux(type, code, year, limit, month, trimester, semester)
    params = {"code": code, "type": check_territory_param(type), "year": year,
              "limit": limit, "month": month, "trimester": trimester, "semester": semester}
    return await _serve_rows(redis, "/observatory/best-flux", params, sql, sp, acquire)


@router.get("/evol-flux")
async def evol_flux(
    code: str = Query(..., min_length=1, max_length=15),
    type: str = Query(...),
    indic: str = Query(..., min_length=1, max_length=32),
    past: int = Query(2, ge=1, le=5),
    month: int | None = Query(None, ge=1, le=12),
    trimester: int | None = Query(None, ge=1, le=4),
    semester: int | None = Query(None, ge=1, le=2),
    acquire=Depends(get_conn),
    redis=Depends(get_redis),
):
    """Évolution temporelle d'un indicateur de flux."""
    check_code_param(code)
    sql, sp = agg.build_evol_flux(type, code, indic, past, month, trimester, semester)
    params = {"code": code, "type": check_territory_param(type),
              "indic": agg.normalize_flux_indic(indic),
              "past": past, "month": month, "trimester": trimester, "semester": semester}
    return await _serve_rows(redis, "/observatory/evol-flux", params, sql, sp, acquire)


@router.get("/incentive")
async def incentive(
    code: str = Query(..., min_length=1, max_length=15),
    type: str = Query(...),
    year: int = Query(..., ge=2020, le=2100),
    month: int | None = Query(None, ge=1, le=12),
    trimester: int | None = Query(None, ge=1, le=4),
    semester: int | None = Query(None, ge=1, le=2),
    acquire=Depends(get_conn),
    redis=Depends(get_redis),
):
    """Répartition des incitations par territoire."""
    check_code_param(code)
    sql, sp = agg.build_incentive(type, code, year, month, trimester, semester)
    params = {"code": code, "type": check_territory_param(type), "year": year,
              "month": month, "trimester": trimester, "semester": semester}
    return await _serve_rows(redis, "/observatory/incentive", params, sql, sp, acquire)


@router.get("/occupation")
async def occupation(
    code: str = Query(..., min_length=1, max_length=15),
    type: str = Query(...),
    observe: str = Query(...),
    year: int = Query(..., ge=2020, le=2100),
    month: int | None = Query(None, ge=1, le=12),
    trimester: int | None = Query(None, ge=1, le=4),
    semester: int | None = Query(None, ge=1, le=2),
    acquire=Depends(get_conn),
    redis=Depends(get_redis),
):
    """Taux d'occupation par territoire (avec géométrie)."""
    check_code_param(code)
    sql, sp = agg.build_occupation(type, observe, code, year, month, trimester, semester)
    params = {"code": code, "type": check_territory_param(type),
              "observe": check_territory_param(observe), "year": year,
              "month": month, "trimester": trimester, "semester": semester}
    return await _serve_rows(redis, "/observatory/occupation", params, sql, sp, acquire)


@router.get("/best-territories")
async def best_territories(
    code: str = Query(..., min_length=1, max_length=15),
    type: str = Query(...),
    observe: str = Query(...),
    year: int = Query(..., ge=2020, le=2100),
    limit: int = Query(10, ge=5, le=100),
    month: int | None = Query(None, ge=1, le=12),
    trimester: int | None = Query(None, ge=1, le=4),
    semester: int | None = Query(None, ge=1, le=2),
    acquire=Depends(get_conn),
    redis=Depends(get_redis),
):
    """Meilleurs territoires par nombre de trajets."""
    check_code_param(code)
    sql, sp = agg.build_best_territories(type, observe, code, year, limit,
                                         month, trimester, semester)
    params = {"code": code, "type": check_territory_param(type),
              "observe": check_territory_param(observe), "year": year, "limit": limit,
              "month": month, "trimester": trimester, "semester": semester}
    return await _serve_rows(redis, "/observatory/best-territories", params, sql, sp, acquire)


@router.get("/evol-occupation")
async def evol_occupation(
    code: str = Query(..., min_length=1, max_length=15),
    type: str = Query(...),
    indic: str = Query(..., min_length=1, max_length=32),
    past: int = Query(2, ge=1, le=5),
    month: int | None = Query(None, ge=1, le=12),
    trimester: int | None = Query(None, ge=1, le=4),
    semester: int | None = Query(None, ge=1, le=2),
    acquire=Depends(get_conn),
    redis=Depends(get_redis),
):
    """Évolution temporelle d'un indicateur d'occupation."""
    check_code_param(code)
    sql, sp = agg.build_evol_occupation(type, code, indic, past, month, trimester, semester)
    params = {"code": code, "type": check_territory_param(type),
              "indic": agg.normalize_occupation_indic(indic),
              "past": past, "month": month, "trimester": trimester, "semester": semester}
    return await _serve_rows(redis, "/observatory/evol-occupation", params, sql, sp, acquire)


@router.get("/journeys-by-hours")
async def journeys_by_hours(
    code: str = Query(..., min_length=1, max_length=15),
    type: str = Query(...),
    year: int = Query(..., ge=2020, le=2100),
    month: int | None = Query(None, ge=1, le=12),
    trimester: int | None = Query(None, ge=1, le=4),
    semester: int | None = Query(None, ge=1, le=2),
    acquire=Depends(get_conn),
    redis=Depends(get_redis),
):
    """Distribution horaire des trajets (toutes directions)."""
    check_code_param(code)
    sql, sp = agg.build_journeys_by_hours(type, code, year, month, trimester, semester)
    params = {"code": code, "type": check_territory_param(type), "year": year,
              "month": month, "trimester": trimester, "semester": semester}
    return await _serve_rows(redis, "/observatory/journeys-by-hours", params, sql, sp, acquire)


@router.get("/journeys-by-distances")
async def journeys_by_distances(
    code: str = Query(..., min_length=1, max_length=15),
    type: str = Query(...),
    year: int = Query(..., ge=2020, le=2100),
    direction: Direction = Query(...),
    month: int | None = Query(None, ge=1, le=12),
    trimester: int | None = Query(None, ge=1, le=4),
    semester: int | None = Query(None, ge=1, le=2),
    acquire=Depends(get_conn),
    redis=Depends(get_redis),
):
    """Distribution kilométrique des trajets (direction requise)."""
    check_code_param(code)
    sql, sp = agg.build_journeys_by_distances(type, code, year, direction,
                                              month, trimester, semester)
    params = {"code": code, "type": check_territory_param(type), "year": year,
              "direction": direction, "month": month,
              "trimester": trimester, "semester": semester}
    return await _serve_rows(redis, "/observatory/journeys-by-distances", params, sql, sp, acquire)


@router.get("/keyfigures")
async def keyfigures(
    code: str = Query(..., min_length=1, max_length=15),
    type: str = Query(...),
    year: int = Query(..., ge=2020, le=2100),
    month: int | None = Query(None, ge=1, le=12),
    trimester: int | None = Query(None, ge=1, le=4),
    semester: int | None = Query(None, ge=1, le=2),
    acquire=Depends(get_conn),
    redis=Depends(get_redis),
):
    """Chiffres clés d'un territoire (recomposition ; direction both)."""
    check_code_param(code)
    sql, sp = agg.build_keyfigures(type, code, year, month, trimester, semester)
    params = {"code": code, "type": check_territory_param(type), "year": year,
              "month": month, "trimester": trimester, "semester": semester}
    return await _serve_rows(redis, "/observatory/keyfigures", params, sql, sp, acquire)


@router.get("/aires-covoiturage")
async def aires_covoiturage(
    type: str = Query("com"),
    code: str | None = Query(None, min_length=1, max_length=15),
    acquire=Depends(get_conn),
    redis=Depends(get_redis),
):
    """Aires de covoiturage ouvertes (optionnellement filtrées par territoire)."""
    if code is not None:
        check_code_param(code)
    sql, sp = agg.build_aires_covoiturage(type, code)
    params = {"type": check_territory_param(type), "code": code}
    return await _serve_rows(redis, "/observatory/aires-covoiturage", params, sql, sp, acquire)
