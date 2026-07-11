import pipelines.helpers.cache as cache
from pipelines.helpers.cache import VERSION_KEY, bump_publication_version, build_client


class FakeRedis:
    def __init__(self, start=0):
        self.store = {}
        self._start = start

    def incr(self, key):
        self.store[key] = self.store.get(key, self._start) + 1
        return self.store[key]


def test_version_key_matches_api():
    # invariant : la clé doit rester identique à api_datalake.cache._VERSION_KEY
    assert VERSION_KEY == "obs:cache:version"


def test_bump_increments_the_version_key():
    r = FakeRedis()
    assert bump_publication_version(r) == 1
    assert bump_publication_version(r) == 2
    assert r.store == {"obs:cache:version": 2}


def test_bump_is_noop_when_cache_disabled(monkeypatch):
    # REDIS_URL absent -> build_client renvoie None -> bump no-op
    monkeypatch.delenv("REDIS_URL", raising=False)
    assert bump_publication_version() is None


def test_build_client_none_without_url(monkeypatch):
    monkeypatch.delenv("REDIS_URL", raising=False)
    assert build_client() is None


def test_build_client_passes_ca_for_rediss(monkeypatch):
    captured = {}

    class FakeRedisModule:
        @staticmethod
        def from_url(url, **kwargs):
            captured["url"] = url
            captured["kwargs"] = kwargs
            return object()

    monkeypatch.setenv("REDIS_URL", "rediss://u:p@h:6379/2")
    monkeypatch.setenv("REDIS_CA", "-----BEGIN CERTIFICATE-----\nX\n-----END CERTIFICATE-----")
    monkeypatch.setitem(__import__("sys").modules, "redis", FakeRedisModule)
    build_client()
    assert captured["kwargs"].get("ssl_ca_data").startswith("-----BEGIN")


def test_build_client_no_tls_args_for_plain_url(monkeypatch):
    captured = {}

    class FakeRedisModule:
        @staticmethod
        def from_url(url, **kwargs):
            captured["kwargs"] = kwargs
            return object()

    monkeypatch.setenv("REDIS_URL", "redis://h:6379/0")
    monkeypatch.delenv("REDIS_CA", raising=False)
    monkeypatch.setitem(__import__("sys").modules, "redis", FakeRedisModule)
    build_client()
    assert "ssl_ca_data" not in captured["kwargs"]
