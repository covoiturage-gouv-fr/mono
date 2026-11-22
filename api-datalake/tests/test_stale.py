import gzip
import json
import logging
from contextlib import asynccontextmanager

from fastapi.testclient import TestClient
from psycopg.errors import QueryCanceled

import api_datalake.cache as cache_mod
from api_datalake.cache import get_redis
from api_datalake.config import settings
from api_datalake.main import create_app
from api_datalake.routers.observatory import get_conn

ROUTE = "/observatory/location"
PARAMS = {"code": "75056", "type": "com", "year": 2022, "n": 8}
STALE_PARAMS = {"code": "75056", "type": "com", "year": 2022, "month": None,
                "trimester": None, "semester": None, "n": 8}


class FakeCursor:
    def __init__(self, rows, error):
        self._rows, self._error = rows, error

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    async def execute(self, sql, params):
        if self._error:
            raise self._error

    async def fetchall(self):
        return self._rows

    async def fetchone(self):
        return None


class FakeConn:
    def __init__(self, rows, error):
        self._rows, self._error = rows, error

    def cursor(self):
        return FakeCursor(self._rows, self._error)


class FakeRedis:
    def __init__(self):
        self.store = {}
        self.ttls = {}

    async def get(self, k):
        return self.store.get(k)

    async def set(self, k, v, ex=None):
        self.store[k] = v
        self.ttls[k] = ex


def make_client(rows=(), redis=None, error=None):
    app = create_app()

    def override_conn():
        @asynccontextmanager
        async def _acquire():
            yield FakeConn(list(rows), error)
        return _acquire

    app.dependency_overrides[get_conn] = override_conn
    app.dependency_overrides[get_redis] = lambda: redis
    return TestClient(app)


def timeout():
    return QueryCanceled("canceling statement due to statement timeout")


def stale_key():
    return cache_mod.build_stale_key(ROUTE, STALE_PARAMS)


def test_stale_key_is_version_independent_and_shares_digest():
    stale = cache_mod.build_stale_key("/a", {"x": 1})
    versioned = cache_mod.build_cache_key("v1", "/a", {"x": 1})
    assert stale.startswith("obs:stale:")
    assert stale.rsplit(":", 1)[1] == versioned.rsplit(":", 1)[1]


def test_miss_writes_versioned_and_stale_keys():
    redis = FakeRedis()
    r = make_client([{"hex": "abc", "count": 1}], redis).get(ROUTE, params=PARAMS)
    assert r.headers["x-cache"] == "MISS"
    assert len(redis.store) == 2
    assert redis.ttls[stale_key()] == settings.cache_stale_ttl_seconds
    other = next(k for k in redis.store if k != stale_key())
    assert redis.store[stale_key()] == redis.store[other]


def test_stale_key_unchanged_across_version_bump():
    redis = FakeRedis()
    c = make_client([{"hex": "a", "count": 1}], redis)
    c.get(ROUTE, params=PARAMS)
    redis.store["obs:cache:version"] = b"2"
    r = c.get(ROUTE, params=PARAMS)
    assert r.headers["x-cache"] == "MISS"  # ancienne clé versionnée inatteignable
    assert stale_key() in redis.store
    assert len([k for k in redis.store if k.startswith("obs:stale:")]) == 1


def test_custom_ttl_writes_no_stale_key():
    redis = FakeRedis()
    r = make_client([], redis).get("/observatory/territories/search", params={"q": "par"})
    assert r.status_code == 200
    assert redis.store and not any(k.startswith("obs:stale:") for k in redis.store)


def test_timeout_serves_stale_when_present():
    redis = FakeRedis()
    payload = [{"hex": "old", "count": 9}]
    redis.store[stale_key()] = gzip.compress(json.dumps(payload).encode())
    r = make_client(redis=redis, error=timeout()).get(ROUTE, params=PARAMS)
    assert r.status_code == 200
    assert r.headers["x-cache"] == "STALE"
    assert r.json() == payload


def test_timeout_without_stale_returns_503():
    r = make_client(redis=FakeRedis(), error=timeout()).get(ROUTE, params=PARAMS)
    assert r.status_code == 503
    assert r.headers["retry-after"] == "120"
    assert r.json() == {"detail": "service temporarily unavailable"}


def test_timeout_without_redis_returns_503():
    r = make_client(redis=None, error=timeout()).get(ROUTE, params=PARAMS)
    assert r.status_code == 503
    assert r.headers["retry-after"] == "120"


def test_global_handler_covers_routes_outside_serve_cached():
    r = make_client(error=timeout()).get("/observatory/last-record", params={"code": "75"})
    assert r.status_code == 503
    assert r.headers["retry-after"] == "120"
    assert r.json() == {"detail": "service temporarily unavailable"}


def test_timeout_logs_warning_without_traceback(caplog):
    with caplog.at_level(logging.WARNING, logger="api_datalake"):
        make_client(redis=None, error=timeout()).get(ROUTE, params=PARAMS)
        make_client(error=timeout()).get("/observatory/last-record", params={"code": "75"})
    records = [r for r in caplog.records if "statement timeout" in r.getMessage()]
    assert len(records) == 2
    assert all(r.exc_info is None for r in records)
