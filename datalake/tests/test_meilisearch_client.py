import requests

from pipelines.helpers.meilisearch_client import build_config, index_documents


class FakeResponse:
    def raise_for_status(self):
        pass


def test_build_config_none_without_host(monkeypatch):
    monkeypatch.delenv("APP_MEILISEARCH_HOST", raising=False)
    assert build_config() is None


def test_build_config_defaults(monkeypatch):
    monkeypatch.setenv("APP_MEILISEARCH_HOST", "http://meili:7700/")
    monkeypatch.delenv("APP_MEILISEARCH_APIKEY", raising=False)
    monkeypatch.delenv("APP_MEILISEARCH_INDEX", raising=False)
    monkeypatch.delenv("APP_MEILISEARCH_BATCH", raising=False)
    config = build_config()
    assert config == {"host": "http://meili:7700", "api_key": "", "index": "geo", "batch_size": 1000}


def test_build_config_reads_overrides(monkeypatch):
    monkeypatch.setenv("APP_MEILISEARCH_HOST", "http://meili:7700")
    monkeypatch.setenv("APP_MEILISEARCH_APIKEY", "secret")
    monkeypatch.setenv("APP_MEILISEARCH_INDEX", "geo_staging")
    monkeypatch.setenv("APP_MEILISEARCH_BATCH", "2")
    config = build_config()
    assert config["api_key"] == "secret"
    assert config["index"] == "geo_staging"
    assert config["batch_size"] == 2


def test_index_documents_sets_filterable_attributes_then_deletes_then_posts_batches(monkeypatch):
    calls = []

    def fake_put(url, headers=None, json=None, timeout=None):
        calls.append(("PUT", url, headers, json))
        return FakeResponse()

    def fake_delete(url, headers=None, timeout=None):
        calls.append(("DELETE", url, headers, None))
        return FakeResponse()

    def fake_post(url, headers=None, json=None, timeout=None):
        calls.append(("POST", url, headers, json))
        return FakeResponse()

    monkeypatch.setattr(requests, "put", fake_put)
    monkeypatch.setattr(requests, "delete", fake_delete)
    monkeypatch.setattr(requests, "post", fake_post)

    config = {"host": "http://meili:7700", "api_key": "secret", "index": "geo", "batch_size": 2}
    documents = [{"id": f"{i}"} for i in range(5)]

    count = index_documents(config, documents)

    assert count == 5
    # ordre : settings d'abord (year/is_latest filtrables), purge, puis lots
    assert calls[0][0] == "PUT"
    assert calls[0][1] == "http://meili:7700/indexes/geo/settings/filterable-attributes"
    assert calls[0][3] == ["year", "is_latest"]
    assert calls[0][2]["Authorization"] == "Bearer secret"

    assert calls[1][0] == "DELETE"
    assert calls[1][1] == "http://meili:7700/indexes/geo/documents"

    posts = [c for c in calls if c[0] == "POST"]
    assert [c[3] for c in posts] == [documents[0:2], documents[2:4], documents[4:5]]
    assert all(c[1] == "http://meili:7700/indexes/geo/documents" for c in posts)


def test_index_documents_omits_auth_header_without_api_key(monkeypatch):
    calls = []
    monkeypatch.setattr(requests, "put", lambda url, headers=None, json=None, timeout=None: calls.append(headers) or FakeResponse())
    monkeypatch.setattr(requests, "delete", lambda url, headers=None, timeout=None: calls.append(headers) or FakeResponse())
    monkeypatch.setattr(requests, "post", lambda url, headers=None, json=None, timeout=None: FakeResponse())

    config = {"host": "http://meili:7700", "api_key": "", "index": "geo", "batch_size": 1000}
    index_documents(config, [{"id": "1"}])

    assert "Authorization" not in calls[0]
    assert "Authorization" not in calls[1]
