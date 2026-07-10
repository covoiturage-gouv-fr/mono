import asyncio
import gzip
import json

import api_datalake.cache as cache_mod
from api_datalake.cache import (
    build_cache_key,
    cache_get,
    cache_set,
    gzip_payload,
    publication_version,
)
from api_datalake.config import settings


def test_build_cache_key_is_stable_and_param_order_independent():
    k1 = build_cache_key("v1", "/observatory/flux", {"code": "75", "type": "reg", "year": 2026})
    k2 = build_cache_key("v1", "/observatory/flux", {"year": 2026, "type": "reg", "code": "75"})
    assert k1 == k2  # l'ordre des params ne change pas la clé


def test_build_cache_key_versioned():
    base = ("/observatory/flux", {"code": "75"})
    assert build_cache_key("v1", *base) != build_cache_key("v2", *base)


def test_build_cache_key_distinguishes_route_and_params():
    assert build_cache_key("v1", "/a", {"x": 1}) != build_cache_key("v1", "/b", {"x": 1})
    assert build_cache_key("v1", "/a", {"x": 1}) != build_cache_key("v1", "/a", {"x": 2})


def test_gzip_payload_roundtrips():
    data = [{"hex": "8818", "count": 3}]
    blob = gzip_payload(data)
    assert isinstance(blob, bytes)
    assert json.loads(gzip.decompress(blob)) == data


# --- ouverture du client Redis : TLS + CA privée ---


def _patch_from_url(monkeypatch):
    """Intercepte redis.asyncio.from_url et renvoie le dict de kwargs capturés."""
    import redis.asyncio as aioredis
    captured = {}

    def fake_from_url(url, **kwargs):
        captured["url"] = url
        captured["kwargs"] = kwargs
        return object()

    monkeypatch.setattr(aioredis, "from_url", fake_from_url)
    monkeypatch.setattr(cache_mod, "_client", None)
    return captured


def test_open_redis_passes_ca_for_rediss(monkeypatch):
    captured = _patch_from_url(monkeypatch)
    monkeypatch.setattr(settings, "redis_url", "rediss://u:p@host:6379/2")
    monkeypatch.setattr(settings, "redis_ca", "-----BEGIN CERTIFICATE-----\nX\n-----END CERTIFICATE-----")
    cache_mod.open_redis()
    assert captured["kwargs"].get("ssl_ca_data") == settings.redis_ca
    monkeypatch.setattr(cache_mod, "_client", None)


def test_open_redis_no_tls_args_for_plain_url(monkeypatch):
    captured = _patch_from_url(monkeypatch)
    monkeypatch.setattr(settings, "redis_url", "redis://host:6379/0")
    monkeypatch.setattr(settings, "redis_ca", None)
    cache_mod.open_redis()
    assert "ssl_ca_data" not in captured["kwargs"]
    monkeypatch.setattr(cache_mod, "_client", None)


def test_open_redis_rediss_without_ca_passes_no_tls_args(monkeypatch):
    # rediss:// sans CA -> pas de kwargs (vérification contre le magasin système).
    captured = _patch_from_url(monkeypatch)
    monkeypatch.setattr(settings, "redis_url", "rediss://host:6379/2")
    monkeypatch.setattr(settings, "redis_ca", None)
    cache_mod.open_redis()
    assert "ssl_ca_data" not in captured["kwargs"]
    monkeypatch.setattr(cache_mod, "_client", None)


# --- dégradation gracieuse : Redis en panne ne lève jamais ---


class RaisingRedis:
    async def get(self, *a, **k):
        raise RuntimeError("redis down")

    async def set(self, *a, **k):
        raise RuntimeError("redis down")


def test_cache_get_returns_not_ok_on_failure():
    val, ok = asyncio.run(cache_get(RaisingRedis(), "k"))
    assert val is None and ok is False


def test_cache_get_off_is_not_a_failure():
    assert asyncio.run(cache_get(None, None)) == (None, True)


def test_cache_set_swallows_failure():
    asyncio.run(cache_set(RaisingRedis(), "k", b"x", 10))  # ne lève pas


def test_publication_version_resilient():
    assert asyncio.run(publication_version(RaisingRedis(), "2026-03-01")) == "2026-03-01.0"
