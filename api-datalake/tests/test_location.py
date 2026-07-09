import gzip
import json

from fastapi.testclient import TestClient

import api_datalake.routers.observatory as router_mod
from api_datalake.cache import get_redis
from api_datalake.main import create_app
from api_datalake.repositories.observatory import build_location_query
from api_datalake.routers.observatory import get_conn

# --- build_location_query (pur, sans DB) ---


def test_query_maps_com_to_arr_column():
    sql, params = build_location_query("com", "75056", 2022, 8, month=6)
    assert "arr = %(code)s" in sql
    assert params["code"] == "75056"


def test_query_maps_region_column_and_binning_resolution():
    sql, params = build_location_query("reg", "11", 2022, 7)
    assert "reg = %(code)s" in sql
    # binning des points de départ ET d'arrivée à la résolution n
    assert "h3_cell_to_parent(start_h3index_z8, %(n)s)" in sql
    assert "h3_cell_to_parent(end_h3index_z8, %(n)s)" in sql
    assert params["n"] == 7


def test_query_unknown_type_falls_back_to_arr():
    sql, _ = build_location_query("pays_imaginaire", "x", 2022, 8)
    assert "arr = %(code)s" in sql  # check_territory_param -> com -> arr


def test_query_period_filters_by_grain():
    _, p_month = build_location_query("com", "75056", 2022, 8, month=6)
    assert p_month["month"] == 6 and "trimester" not in p_month

    sql_t, p_tri = build_location_query("com", "75056", 2022, 8, trimester=2)
    assert "EXTRACT(QUARTER FROM start_datetime) = %(trimester)s" in sql_t
    assert p_tri["trimester"] == 2


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


def make_client(rows, redis=None):
    app = create_app()

    async def override_conn():
        yield FakeConn(rows)

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


def test_location_out_of_range_params_return_422_not_500():
    c = TestClient(make_client([]))
    # month=13 est hors bornes -> 422 de validation, pas un 500 (ValueError sur date())
    r = c.get("/observatory/location", params={"code": "75056", "type": "com", "year": 2022, "month": 13, "n": 8})
    assert r.status_code == 422


def test_location_unpublished_period_is_bypassed_and_empty():
    c = TestClient(make_client([{"hex": "x", "count": 9}]))
    router_mod.settings.app_observatory_published_until = "2022-01-01"
    try:
        r = c.get("/observatory/location", params={"code": "75056", "type": "com", "year": 2022, "month": 6, "n": 8})
        assert r.headers["x-cache"] == "BYPASS"
        assert r.json() == []
    finally:
        router_mod.settings.app_observatory_published_until = None
