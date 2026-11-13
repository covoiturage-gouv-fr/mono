import io
import json
import logging
from contextlib import asynccontextmanager

import pytest
from fastapi import Request
from fastapi.testclient import TestClient

import api_datalake.main as main_mod
from api_datalake.access_log import access_logger, clean_header
from api_datalake.cache import get_redis
from api_datalake.config import Settings
from api_datalake.main import create_app
from api_datalake.routers.observatory import get_conn

from .test_health import _DownPool, _OkPool


class _FakeCursor:
    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    async def execute(self, sql, params):
        pass

    async def fetchall(self):
        return [{"hex": "abc", "count": 1}]


class _FakeConn:
    def cursor(self):
        return _FakeCursor()


LOCATION = {"code": "75056", "type": "com", "year": 2022, "n": 8}


@pytest.fixture
def out():
    """Flux qui reçoit les lignes du journal d'accès, telles qu'écrites sur stdout."""
    stream = io.StringIO()
    handler = logging.StreamHandler(stream)
    access_logger.addHandler(handler)
    yield stream
    access_logger.removeHandler(handler)


def make_client(maintenance=False):
    app = create_app()
    app.state.settings = Settings(maintenance_mode=maintenance)

    def override_conn():
        @asynccontextmanager
        async def _acquire():
            yield _FakeConn()
        return _acquire

    app.dependency_overrides[get_conn] = override_conn
    app.dependency_overrides[get_redis] = lambda: None

    @app.get("/_test/boom")
    async def boom():
        raise RuntimeError("boom")

    @app.get("/_test/me/{item}")
    async def me(item: str, request: Request):
        request.state.sub = "user-42"
        request.state.role = "territory.admin"
        return {}

    return TestClient(app, raise_server_exceptions=False)


def lines(out):
    return [json.loads(line) for line in out.getvalue().splitlines()]


def test_one_json_line_with_expected_fields(out):
    r = make_client().get("/observatory/location", params=LOCATION, headers={"X-Request-Id": "abc123"})
    assert r.status_code == 200
    (entry,) = lines(out)
    assert entry["level"] == "info"
    assert entry["ip"] == "testclient"
    assert entry["request_id"] == "abc123"
    assert entry["method"] == "GET"
    assert entry["route"] == "/observatory/location"
    assert entry["status"] == 200
    assert isinstance(entry["duration_ms"], int) and entry["duration_ms"] >= 0
    assert "ts" in entry
    for absent in ("pentest_id", "sub", "role"):
        assert absent not in entry


def test_route_is_template_not_raw_path(out):
    make_client().get("/_test/me/secret-value")
    (entry,) = lines(out)
    assert entry["route"] == "/_test/me/{item}"
    assert "secret-value" not in out.getvalue()


def test_sub_and_role_from_request_state(out):
    make_client().get("/_test/me/x")
    (entry,) = lines(out)
    assert entry["sub"] == "user-42"
    assert entry["role"] == "territory.admin"


def test_unserialisable_state_does_not_break_the_request(out):
    client = make_client()

    @client.app.get("/_test/odd")
    async def odd(request: Request):
        request.state.sub = object()
        return {}

    assert client.get("/_test/odd").status_code == 200
    (entry,) = lines(out)
    assert entry["sub"].startswith("<object")


def test_pentest_id_only_when_header_present(out):
    make_client().get("/observatory/location", params=LOCATION, headers={"X-Pentest-Id": "EY-JD"})
    (entry,) = lines(out)
    assert entry["pentest_id"] == "EY-JD"


def test_request_id_omitted_when_absent(out):
    make_client().get("/observatory/location", params=LOCATION)
    (entry,) = lines(out)
    assert "request_id" not in entry


def test_nothing_sensitive_is_logged(out):
    make_client().get(
        "/observatory/location",
        params={**LOCATION, "code": "2A004"},
        headers={"Authorization": "Bearer s3cr3t-token", "Cookie": "session=c00kie"},
    )
    raw = out.getvalue()
    for leaked in ("2A004", "code=", "?", "s3cr3t-token", "Bearer", "c00kie"):
        assert leaked not in raw


def test_header_values_are_sanitised(out):
    make_client().get(
        "/observatory/location",
        params=LOCATION,
        headers={"X-Request-Id": 'a"b c{d}<e>', "X-Pentest-Id": "x" * 500},
    )
    assert len(out.getvalue().splitlines()) == 1
    (entry,) = lines(out)
    assert entry["request_id"] == "abcde"
    assert entry["pentest_id"] == "x" * 64


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("0af1-B2_c.3", "0af1-B2_c.3"),
        ('evil\r\n{"status":200}', "evilstatus200"),
        ("é\x00\x1b[31m", "31m"),
        ("   ", None),
        ("", None),
        (None, None),
    ],
)
def test_clean_header(raw, expected):
    assert clean_header(raw) == expected


def test_status_404_has_no_route_nor_raw_path(out):
    r = make_client().get("/nope/secret-path", params={"q": "secret"})
    assert r.status_code == 404
    (entry,) = lines(out)
    assert entry["status"] == 404
    assert "route" not in entry
    assert "secret" not in out.getvalue()


def test_status_422(out):
    r = make_client().get("/observatory/location", params={**LOCATION, "month": 13})
    assert r.status_code == 422
    (entry,) = lines(out)
    assert entry["status"] == 422
    assert entry["route"] == "/observatory/location"


def test_status_503_in_maintenance(out):
    r = make_client(maintenance=True).get("/observatory/location", params=LOCATION)
    assert r.status_code == 503
    (entry,) = lines(out)
    assert entry["status"] == 503
    assert "route" not in entry  # court-circuité avant le routeur


def test_status_500_on_unhandled_exception(out):
    r = make_client().get("/_test/boom")
    assert r.status_code == 500
    (entry,) = lines(out)
    assert entry["status"] == 500
    assert entry["route"] == "/_test/boom"


def test_successful_probes_are_not_logged(out, monkeypatch):
    monkeypatch.setattr(main_mod, "open_pool", _OkPool)
    c = make_client()
    assert c.get("/health").status_code == 200
    assert c.get("/health/ready").status_code == 200
    assert out.getvalue() == ""


def test_failing_probe_is_logged(out, monkeypatch):
    monkeypatch.setattr(main_mod, "open_pool", _DownPool)
    assert make_client().get("/health/ready").status_code == 503
    (entry,) = lines(out)
    assert entry["status"] == 503
    assert entry["route"] == "/health/ready"
