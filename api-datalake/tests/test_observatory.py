from fastapi.testclient import TestClient

from api_datalake.main import create_app
from api_datalake.routers.observatory import get_conn


class FakeCursor:
    def __init__(self, row):
        self._row = row
        self.executed = None

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    async def execute(self, sql, params):
        self.executed = (sql, params)

    async def fetchone(self):
        return self._row


class FakeConn:
    def __init__(self, row):
        self._row = row

    def cursor(self):
        return FakeCursor(self._row)


def client_with_row(row):
    app = create_app()

    async def override():
        yield FakeConn(row)

    app.dependency_overrides[get_conn] = override
    return TestClient(app)


def test_last_record_returns_year_month():
    c = client_with_row({"year": 2026, "month": 2})
    r = c.get("/v3/observatory/last-record", params={"code": "75", "type": "reg"})
    assert r.status_code == 200
    assert r.json() == {"year": 2026, "month": 2}


def test_last_record_returns_null_when_no_data():
    c = client_with_row(None)
    r = c.get("/v3/observatory/last-record", params={"code": "00000", "type": "com"})
    assert r.status_code == 200
    assert r.json() is None


def test_health():
    c = client_with_row(None)
    assert c.get("/health").json() == {"status": "ok"}
