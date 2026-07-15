from contextlib import asynccontextmanager

import pytest
from fastapi.testclient import TestClient

from api_datalake.cache import get_redis
from api_datalake.config import Settings
from api_datalake.main import create_app
from api_datalake.routers.observatory import get_conn


class _FakeCursor:
    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    async def execute(self, sql, params):
        pass

    async def fetchall(self):
        return []


class _FakeConn:
    def cursor(self):
        return _FakeCursor()


def app_with_maintenance(on, fake_db=False):
    app = create_app()
    app.state.settings = Settings(maintenance_mode=on)  # isolé du singleton
    if fake_db:
        def override_conn():
            @asynccontextmanager
            async def _acquire():
                yield _FakeConn()
            return _acquire

        app.dependency_overrides[get_conn] = override_conn
        app.dependency_overrides[get_redis] = lambda: None
    return TestClient(app)


def test_off_serves_routes_normally():
    # /health (exempt) + une vraie route observatoire servie depuis une DB factice.
    c = app_with_maintenance(False, fake_db=True)
    assert c.get("/health").status_code == 200
    assert c.get("/observatory/campaigns", params={"year": 2024}).status_code == 200


def test_on_returns_503_with_retry_after_and_body():
    c = app_with_maintenance(True)
    r = c.get("/observatory/campaigns", params={"year": 2024})
    assert r.status_code == 503
    assert r.headers["Retry-After"] == "3600"
    assert r.json() == {"status": "maintenance"}


def test_on_short_circuits_observatory_before_db():
    # Aucune dépendance DB surchargée : si la route s'exécutait, elle ouvrirait le pool.
    # Le 503 prouve le court-circuit avant tout accès PG/Redis.
    c = app_with_maintenance(True)
    r = c.get("/observatory/campaigns", params={"year": 2024})
    assert r.status_code == 503


def test_on_health_stays_200():
    c = app_with_maintenance(True)
    r = c.get("/health")
    assert r.status_code == 200
    assert r.json() == {"status": "ok"}


@pytest.mark.parametrize("value", ["true", "TRUE", "1", "on", "yes", "  On  "])
def test_parsing_truthy(value):
    assert Settings(maintenance_mode=value).maintenance_mode is True


@pytest.mark.parametrize("value", ["", "false", "0", "no", "off", "nope"])
def test_parsing_falsy(value):
    assert Settings(maintenance_mode=value).maintenance_mode is False
