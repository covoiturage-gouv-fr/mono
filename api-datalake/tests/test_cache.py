import gzip
import json

from api_datalake.cache import build_cache_key, gzip_payload


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
