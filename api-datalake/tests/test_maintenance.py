import pytest
from fastapi.testclient import TestClient

from api_datalake.config import Settings
from api_datalake.main import create_app


def app_with_maintenance(on):
    app = create_app()
    app.state.settings = Settings(maintenance_mode=on)  # isolé du singleton
    return TestClient(app)


def test_off_serves_routes_normally():
    c = app_with_maintenance(False)
    assert c.get("/openapi.json").status_code == 200
    assert c.get("/health").status_code == 200


def test_on_returns_503_with_retry_after_and_body():
    c = app_with_maintenance(True)
    r = c.get("/openapi.json")
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
