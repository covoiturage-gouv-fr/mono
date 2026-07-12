import gzip
import json

from fastapi.testclient import TestClient

from api_datalake.cache import get_redis
from api_datalake.main import create_app
from api_datalake.observatory_sql import perimeter_in_subquery, resolve_grain
from api_datalake.repositories import observatory_aggregated as agg
from api_datalake.routers.observatory import get_conn

ALL_BUILDERS = [
    ("build_flux", agg.build_flux, ("reg", "reg", "11", 2022, 6, None, None)),
    ("build_best_flux", agg.build_best_flux, ("reg", "11", 2022, 10, None, None, None)),
    ("build_evol_flux", agg.build_evol_flux, ("reg", "11", "journeys", 2, 6, None, None)),
    ("build_incentive", agg.build_incentive, ("reg", "11", 2022, 6, None, None)),
    ("build_occupation", agg.build_occupation, ("reg", "reg", "11", 2022, 6, None, None)),
    ("build_best_territories", agg.build_best_territories, ("reg", "reg", "11", 2022, 10, None, None, None)),
    ("build_evol_occupation", agg.build_evol_occupation, ("reg", "11", "journeys", 2, 6, None, None)),
    ("build_journeys_by_hours", agg.build_journeys_by_hours, ("reg", "11", 2022, 6, None, None)),
    ("build_journeys_by_distances", agg.build_journeys_by_distances, ("reg", "11", 2022, "both", 6, None, None)),
    ("build_keyfigures", agg.build_keyfigures, ("reg", "11", 2022, 6, None, None)),
    ("build_aires_covoiturage", agg.build_aires_covoiturage, ("dep", "33")),
]


# --- garde-fou architectural : l'API ne lit QUE la zone exposée ---


def test_all_builders_read_only_exposed_zone():
    for name, fn, args in ALL_BUILDERS:
        sql, _ = fn(*args)
        for forbidden in ("zone_trusted.", "zone_raw.", "zone_aggregated."):
            assert forbidden not in sql, f"{name} lit {forbidden}"
        assert "zone_exposed." in sql, f"{name} ne lit pas zone_exposed"


# --- resolve_grain : priorité + renommage trimester -> quarter ---


def test_resolve_grain_priority_and_quarter_rename():
    assert resolve_grain(6, None, None) == ("month", "month", 6)
    assert resolve_grain(None, 2, None) == ("quarter", "quarter", 2)  # trimester -> quarter
    assert resolve_grain(None, None, 1) == ("semester", "semester", 1)
    assert resolve_grain(None, None, None) == ("year", None, None)
    # month l'emporte sur les autres
    assert resolve_grain(6, 2, 1)[0] == "month"


def test_perimeter_subquery_reads_exposed_perimeters():
    sub = perimeter_in_subquery("epci", "reg")
    assert "zone_exposed.observatory_perimeters" in sub
    assert "geo.perimeters" not in sub
    assert "t.epci" in sub and "t.reg = %(code)s" in sub


# --- structure des requêtes (grain + filtres clés) ---


def test_flux_uses_od_table_and_kanon_filter():
    sql, p = agg.build_flux("reg", "reg", "11", 2022, month=6)
    assert "zone_exposed.od_month" in sql
    assert "(distance / journeys) <= 80" in sql
    assert "territory_1 <> territory_2" in sql
    assert "type = %(observe)s" in sql        # filtre type = observe (lié)
    assert p == {"year": 2022, "code": "11", "tval": 6, "observe": "reg"}


def test_flux_quarter_grain_maps_od_quarter():
    sql, _ = agg.build_flux("reg", "reg", "11", 2022, trimester=2)
    assert "zone_exposed.od_quarter" in sql
    assert "quarter = %(tval)s" in sql


def test_incentive_emits_both_direction_constant():
    sql, _ = agg.build_incentive("reg", "11", 2022, month=6)
    assert "'both'::text AS direction" in sql
    assert "zone_exposed.incentive_month" in sql


def test_distances_requires_direction_filter():
    sql, p = agg.build_journeys_by_distances("reg", "11", 2022, "from", month=6)
    assert "direction = %(direction)s" in sql
    assert p["direction"] == "from"
    # hours ne filtre pas la direction
    sql_h, _ = agg.build_journeys_by_hours("reg", "11", 2022, month=6)
    assert "direction = %(direction)s" not in sql_h


def test_evol_indic_allowlist_fallback_journeys():
    # indic inconnu -> fallback journeys (garde anti-injection)
    sql, _ = agg.build_evol_flux("reg", "11", "DROP TABLE", 2, month=6)
    assert "sum(journeys::numeric) AS journeys" in sql
    assert "DROP TABLE" not in sql


def test_evol_flux_limit_is_past_times_12_plus_1():
    _, p = agg.build_evol_flux("reg", "11", "journeys", past=3, month=6)
    assert p["limit"] == 37


def test_keyfigures_composes_od_occupation_users():
    sql, _ = agg.build_keyfigures("reg", "11", 2022, month=6)
    assert "zone_exposed.od_month" in sql
    assert "zone_exposed.occupation_month" in sql
    assert "zone_exposed.users_month" in sql
    assert "intra_journeys" in sql


def test_aires_optional_code_filter():
    sql_all, p_all = agg.build_aires_covoiturage("com", None)
    assert "insee IN" not in sql_all and p_all == {}
    sql_code, p_code = agg.build_aires_covoiturage("dep", "33")
    assert "insee IN" in sql_code and p_code == {"code": "33"}


# --- endpoints (fake DB) ---


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


def client(rows):
    app = create_app()

    async def override():
        yield FakeConn(rows)

    app.dependency_overrides[get_conn] = override
    app.dependency_overrides[get_redis] = lambda: None
    return TestClient(app)


def test_flux_endpoint_gzipped_rows():
    rows = [{"ter_1": "Île-de-France", "passengers": 1198}]
    c = client(rows)
    r = c.get("/v3/observatory/flux", params={"code": "11", "type": "reg", "observe": "reg", "year": 2022, "month": 6})
    assert r.status_code == 200
    assert r.headers["content-encoding"] == "gzip"
    assert r.json() == rows


def test_keyfigures_endpoint():
    rows = [{"code": "11", "journeys": 5, "intra_journeys": 2}]
    c = client(rows)
    r = c.get("/v3/observatory/keyfigures", params={"code": "11", "type": "reg", "year": 2022, "month": 6})
    assert r.status_code == 200
    assert r.json() == rows


def test_aires_endpoint_without_code():
    rows = [{"id_lieu": "A1", "nom_lieu": "Aire test", "geom": {"type": "Point"}}]
    c = client(rows)
    r = c.get("/v3/observatory/aires-covoiturage", params={"type": "com"})
    assert r.status_code == 200
    assert r.json() == rows


def test_journeys_by_distances_requires_direction():
    c = client([])
    r = c.get("/v3/observatory/journeys-by-distances", params={"code": "11", "type": "reg", "year": 2022, "month": 6})
    assert r.status_code == 422  # direction manquante


def test_invalid_code_returns_422():
    c = client([])
    r = c.get("/v3/observatory/flux", params={"code": "bad!code", "type": "reg", "observe": "reg", "year": 2022})
    assert r.status_code == 422
