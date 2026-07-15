import asyncio

from fastapi.testclient import TestClient

from api_datalake.cache import get_redis
from api_datalake.main import create_app
from api_datalake.repositories.observatory import (
    CAMPAIGNS_COLUMNS,
    build_campaigns_query,
)
from api_datalake.routers.observatory import get_conn


class FakeCursor:
    def __init__(self, rows, delay=0.0):
        self._rows = rows
        self._delay = delay

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    async def execute(self, sql, params):
        if self._delay:
            await asyncio.sleep(self._delay)

    async def fetchall(self):
        return self._rows

    async def fetchone(self):
        return self._rows[0] if self._rows else None


class FakeConn:
    def __init__(self, rows=None, delay=0.0):
        self._rows = rows or []
        self._delay = delay

    def cursor(self):
        return FakeCursor(self._rows, self._delay)


def client(rows=None, delay=0.0):
    app = create_app()

    async def override_conn():
        yield FakeConn(rows, delay)

    app.dependency_overrides[get_conn] = override_conn
    app.dependency_overrides[get_redis] = lambda: None
    return TestClient(app, raise_server_exceptions=False)


# --- 1. docs / openapi coupés ---


def test_openapi_and_docs_are_404():
    c = client()
    assert c.get("/openapi.json").status_code == 404
    assert c.get("/docs").status_code == 404
    assert c.get("/redoc").status_code == 404


# --- 1bis. erreurs de validation laconiques (pas d'écho de l'entrée) ---


def test_validation_error_is_terse_and_does_not_echo_input():
    c = client()
    r = c.get("/observatory/location", params={"code": "75056", "type": "com", "year": 2022, "month": 13, "n": 8})
    assert r.status_code == 422
    assert r.json() == {"detail": "invalid request parameters"}
    assert "13" not in r.text  # la valeur invalide ne fuit pas


# --- 2. projection explicite des colonnes de campaigns ---


def test_campaigns_query_projects_explicit_allowlist():
    sql, _ = build_campaigns_query(type_="aom", code="217500016", year=2024)
    assert "SELECT *" not in sql
    assert "select *" not in sql.lower()
    for col in CAMPAIGNS_COLUMNS:
        assert col in sql
    # colonnes internes du seed jamais exposées
    for leaked in ("email", "collectivite", "siren_et_code_région"):
        assert leaked not in CAMPAIGNS_COLUMNS


# --- 3. validation du paramètre code (régression du garde-fou #3267) ---
# Note : la validation du code (check_code_param, charset alphanumérique borné) et le
# timeout par requête (statement_timeout PG côté session) sont déjà livrés par #3267 ;
# on garde ici une régression légère, sans re-implémenter.


def test_invalid_code_charset_returns_422():
    c = client()
    r = c.get("/observatory/location", params={"code": "75%56", "type": "com", "year": 2022, "n": 8})
    assert r.status_code == 422


def test_too_long_code_returns_422():
    c = client()
    r = c.get("/observatory/location", params={"code": "1234567890123456", "type": "com", "year": 2022, "n": 8})
    assert r.status_code == 422


def test_valid_corsican_code_accepted():
    # 2A004 (arrondissement corse) : alphanumérique valide, atteint la requête.
    c = client(rows=[{"hex": "abc", "count": 1}])
    r = c.get("/observatory/location", params={"code": "2A004", "type": "com", "year": 2022, "n": 8})
    assert r.status_code == 200


def test_campaigns_invalid_code_returns_422():
    c = client()
    r = c.get("/observatory/campaigns", params={"code": "bad!", "year": 2024})
    assert r.status_code == 422
