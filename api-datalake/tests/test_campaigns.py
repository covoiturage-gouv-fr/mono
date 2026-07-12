from fastapi.testclient import TestClient

from api_datalake.cache import get_redis
from api_datalake.main import create_app
from api_datalake.repositories.observatory import (
    build_campaigns_query,
    build_location_query,
)
from api_datalake.routers.observatory import get_conn

# --- garde-fou architectural : l'API ne lit QUE la zone exposée ---


def test_queries_only_read_exposed_zone():
    loc_sql, _ = build_location_query("dep", "75", 2022, 8, month=6)
    camp_sql, _ = build_campaigns_query("aom", "217500016", 2024)
    for sql in (loc_sql, camp_sql):
        assert "zone_trusted." not in sql
        assert "zone_raw." not in sql
        assert "zone_aggregated." not in sql
        assert "zone_exposed." in sql


# --- build_campaigns_query : branches de filtre (parité API) ---


def test_campaigns_no_params_returns_future_campaigns():
    sql, params = build_campaigns_query()
    assert "date_fin > now()" in sql
    assert "geom IS NOT NULL" in sql
    assert params == {}


def test_campaigns_year_only_adds_past_bound():
    sql, params = build_campaigns_query(year=2024)
    assert "EXTRACT(YEAR FROM date_fin) = %(year)s" in sql
    assert "date_fin < now()" in sql
    assert params["year"] == 2024


def test_campaigns_year_and_code_no_past_bound():
    sql, params = build_campaigns_query(code="217500016", year=2024)
    assert "left(code, 9) = %(code)s" in sql
    assert "date_fin < now()" not in sql
    assert params == {"code": "217500016", "year": 2024}


def test_campaigns_type_is_allowlisted():
    _, params = build_campaigns_query(type_="pays_imaginaire")
    assert params["type"] == "com"  # fallback


# --- endpoint /observatory/campaigns ---


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


def test_campaigns_endpoint_returns_gzipped_list():
    rows = [{"type": "aom", "code": "217500016", "lien": "https://x", "geom": {"type": "Polygon"}}]
    app = create_app()

    async def override_conn():
        yield FakeConn(rows)

    app.dependency_overrides[get_conn] = override_conn
    app.dependency_overrides[get_redis] = lambda: None
    c = TestClient(app)
    r = c.get("/v3/observatory/campaigns", params={"type": "aom", "code": "217500016", "year": 2024})
    assert r.status_code == 200
    assert r.headers["content-encoding"] == "gzip"
    assert r.json() == rows
