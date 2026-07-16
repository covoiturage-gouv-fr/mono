import gzip
import json
from contextlib import asynccontextmanager

from fastapi.testclient import TestClient

from api_datalake.cache import get_redis
from api_datalake.config import settings
from api_datalake.main import create_app
from api_datalake.repositories.observatory import build_location_query
from api_datalake.routers.observatory import get_conn

# --- build_location_query (pur, sans DB) ---


def test_query_reads_exposed_aggregate_and_bins():
    sql, params = build_location_query("reg", "11", 2022, 7, month=6)
    assert "FROM zone_exposed.location_month" in sql
    assert "h3_cell_to_parent(hex_z8, %(n)s)" in sql
    assert params["type"] == "reg" and params["code"] == "11" and params["n"] == 7


def test_query_filters_type_code_year():
    sql, params = build_location_query("com", "75056", 2022, 8, month=6)
    assert "type = %(type)s" in sql and "code = %(code)s" in sql and "year = %(year)s" in sql
    assert params["type"] == "com" and params["code"] == "75056"


def test_query_unknown_type_falls_back_to_com():
    _, params = build_location_query("pays_imaginaire", "x", 2022, 8, month=6)
    assert params["type"] == "com"  # check_territory_param -> com


def test_query_grain_selects_table_and_filter():
    sql_m, p_m = build_location_query("com", "75056", 2022, 8, month=6)
    assert "location_month" in sql_m and "month = %(grain_val)s" in sql_m and p_m["grain_val"] == 6

    sql_t, p_t = build_location_query("com", "75056", 2022, 8, trimester=2)
    assert "location_quarter" in sql_t and "quarter = %(grain_val)s" in sql_t and p_t["grain_val"] == 2

    sql_s, p_s = build_location_query("com", "75056", 2022, 8, semester=1)
    assert "location_semester" in sql_s and "semester = %(grain_val)s" in sql_s and p_s["grain_val"] == 1

    sql_y, p_y = build_location_query("com", "75056", 2022, 8)  # sans grain -> année
    assert "location_year" in sql_y and "grain_val" not in p_y


# --- endpoint /observatory/location ---


class FakeCursor:
    def __init__(self, rows):
        self._rows = rows

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    async def execute(self, sql, params):
        pass

    async def fetchall(self):
        return self._rows


class FakeConn:
    def __init__(self, rows):
        self._rows = rows

    def cursor(self):
        return FakeCursor(self._rows)


class FakeRedis:
    def __init__(self):
        self.store = {}

    async def get(self, k):
        return self.store.get(k)

    async def set(self, k, v, ex=None):
        self.store[k] = v


class RaisingRedis:
    async def get(self, *a, **k):
        raise RuntimeError("redis down")

    async def set(self, *a, **k):
        raise RuntimeError("redis down")


def make_client(rows, redis=None):
    app = create_app()

    def override_conn():
        @asynccontextmanager
        async def _acquire():
            yield FakeConn(rows)
        return _acquire

    app.dependency_overrides[get_conn] = override_conn
    app.dependency_overrides[get_redis] = lambda: redis
    return app


def test_location_returns_gzipped_heatmap():
    rows = [{"hex": "881fb46461fffff", "count": 616}]
    c = TestClient(make_client(rows))
    r = c.get("/observatory/location", params={"code": "75056", "type": "com", "year": 2022, "month": 6, "n": 8})
    assert r.status_code == 200
    assert r.headers["content-encoding"] == "gzip"
    assert r.headers["x-cache"] == "MISS"
    assert r.json() == rows  # httpx décompresse gzip de façon transparente


def test_location_cache_hit_served_from_redis():
    redis = FakeRedis()
    cached = [{"hex": "abc", "count": 1}]
    c = TestClient(make_client([], redis=redis))
    # 1er appel : MISS, remplit le cache (data = [] côté DB)
    first = c.get("/observatory/location", params={"code": "75056", "type": "com", "year": 2022, "n": 8})
    assert first.headers["x-cache"] == "MISS"
    # on force une entrée pré-existante et on rappelle : HIT
    (only_key,) = redis.store.keys()
    redis.store[only_key] = gzip.compress(json.dumps(cached).encode())
    second = c.get("/observatory/location", params={"code": "75056", "type": "com", "year": 2022, "n": 8})
    assert second.headers["x-cache"] == "HIT"
    assert second.json() == cached


def test_location_redis_failure_degrades_to_pg_not_500():
    # Redis en panne (TLS/réseau) : on sert depuis PG en 200, X-Cache BYPASS, jamais 500.
    rows = [{"hex": "881fb46461fffff", "count": 616}]
    c = TestClient(make_client(rows, redis=RaisingRedis()))
    r = c.get("/observatory/location", params={"code": "75056", "type": "com", "year": 2022, "month": 6, "n": 8})
    assert r.status_code == 200
    assert r.headers["x-cache"] == "BYPASS"
    assert r.json() == rows


def test_location_out_of_range_params_return_422_not_500():
    c = TestClient(make_client([]))
    # month=13 est hors bornes -> 422 de validation, pas un 500 (ValueError sur date())
    r = c.get("/observatory/location", params={"code": "75056", "type": "com", "year": 2022, "month": 13, "n": 8})
    assert r.status_code == 422


def test_location_rejects_malformed_code_with_422():
    c = TestClient(make_client([]))
    # Code hors charset/longueur : rejeté avant tout accès PG (anti cache-flooding).
    r = c.get("/observatory/location", params={"code": "75'; DROP--", "type": "com", "year": 2022, "n": 8})
    assert r.status_code == 422
    r_long = c.get("/observatory/location", params={"code": "x" * 20, "type": "com", "year": 2022, "n": 8})
    assert r_long.status_code == 422
    # `$` laisserait passer un newline final ; `fullmatch` le rejette.
    r_nl = c.get("/observatory/location", params={"code": "75056\n", "type": "com", "year": 2022, "n": 8})
    assert r_nl.status_code == 422


def test_location_cache_hit_opens_no_connection():
    # 1er appel : MISS, remplit le cache. 2e : HIT — l'acquéreur ne doit jamais
    # être ouvert. On le remplace alors par un acquéreur qui lève à l'ouverture.
    redis = FakeRedis()
    app = make_client([{"hex": "a", "count": 1}], redis=redis)
    c = TestClient(app)
    p = {"code": "75056", "type": "com", "year": 2022, "month": 6, "n": 8}

    first = c.get("/observatory/location", params=p)
    assert first.headers["x-cache"] == "MISS"

    def exploding():
        @asynccontextmanager
        async def _acquire():
            raise AssertionError("pool sollicité sur un cache HIT")
            yield  # pragma: no cover
        return _acquire
    app.dependency_overrides[get_conn] = exploding

    second = c.get("/observatory/location", params=p)
    assert second.status_code == 200
    assert second.headers["x-cache"] == "HIT"
    assert second.json() == [{"hex": "a", "count": 1}]


def test_location_unpublished_period_is_bypassed_and_empty():
    c = TestClient(make_client([{"hex": "x", "count": 9}]))
    settings.app_observatory_published_until = "2022-01-01"
    try:
        r = c.get("/observatory/location", params={"code": "75056", "type": "com", "year": 2022, "month": 6, "n": 8})
        assert r.headers["x-cache"] == "BYPASS"
        assert r.json() == []
    finally:
        settings.app_observatory_published_until = None
