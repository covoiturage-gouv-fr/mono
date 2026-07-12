"""Le préfixe d'URL /v3 EST la version de contrat de l'API publique."""

from fastapi.testclient import TestClient

from api_datalake.cache import get_redis
from api_datalake.main import API_VERSION, create_app
from api_datalake.routers.observatory import get_conn


class _FakeCursor:
    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    async def execute(self, sql, params):
        pass

    async def fetchone(self):
        return {"year": 2026, "month": 2}


class _FakeConn:
    def cursor(self):
        return _FakeCursor()


def _client():
    app = create_app()

    async def override():
        yield _FakeConn()

    app.dependency_overrides[get_conn] = override
    app.dependency_overrides[get_redis] = lambda: None
    return TestClient(app)


def test_version_is_v3():
    assert API_VERSION == "v3"


def test_observatory_served_under_v3():
    c = _client()
    r = c.get("/v3/observatory/last-record", params={"code": "11", "type": "reg"})
    assert r.status_code == 200


def test_unversioned_observatory_is_404():
    # ne jamais servir hors /v3 : casserait le namespace de contrat
    c = _client()
    assert c.get("/observatory/last-record", params={"code": "11", "type": "reg"}).status_code == 404


def test_health_stays_unversioned():
    # sondes d'ops : hors du contrat public, non versionnées
    c = _client()
    assert c.get("/health").status_code == 200
    assert c.get("/v3/health").status_code == 404
