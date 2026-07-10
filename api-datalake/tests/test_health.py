from fastapi.testclient import TestClient

import api_datalake.main as main_mod


class _Cursor:
    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    async def execute(self, sql):
        pass

    async def fetchone(self):
        return (1,)


class _Conn:
    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    def cursor(self):
        return _Cursor()


class _OkPool:
    def connection(self):
        return _Conn()


class _DownPool:
    def connection(self):
        raise RuntimeError("pool closed / db unreachable")


def test_liveness_always_ok():
    c = TestClient(main_mod.create_app())
    r = c.get("/health")
    assert r.status_code == 200 and r.json() == {"status": "ok"}


def test_readiness_ok_when_db_answers(monkeypatch):
    monkeypatch.setattr(main_mod, "open_pool", _OkPool)
    c = TestClient(main_mod.create_app())
    r = c.get("/health/ready")
    assert r.status_code == 200 and r.json() == {"status": "ready"}


def test_readiness_503_when_db_unreachable(monkeypatch):
    monkeypatch.setattr(main_mod, "open_pool", _DownPool)
    c = TestClient(main_mod.create_app())
    r = c.get("/health/ready")
    assert r.status_code == 503 and r.json() == {"status": "unavailable"}
