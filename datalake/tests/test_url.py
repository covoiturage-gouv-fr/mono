import pytest

from pipelines.helpers import url


class FakeResponse:
    """requests.Response minimal pour piloter iter_content et le header."""

    def __init__(self, chunks, content_length=None):
        self._chunks = chunks
        self.headers = {}
        if content_length is not None:
            self.headers["Content-Length"] = str(content_length)

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def raise_for_status(self):
        pass

    def iter_content(self, chunk_size):
        yield from self._chunks


def _patch_get(monkeypatch, response):
    monkeypatch.setattr("time.sleep", lambda s: None)
    monkeypatch.setattr(url.requests, "get", lambda *a, **k: response)


def test_download_url_rejects_non_https():
    with pytest.raises(ValueError, match="https"):
        url.download_url("http://example.com/x.csv", "csv")
    with pytest.raises(ValueError, match="https"):
        url.download_url("file:///etc/passwd", "csv")


def test_download_url_writes_https_file(monkeypatch):
    _patch_get(monkeypatch, FakeResponse([b"col\n", b"val\n"]))
    path = url.download_url("https://example.com/x.csv", "csv")
    with open(path, "rb") as f:
        assert f.read() == b"col\nval\n"


def test_download_url_aborts_when_over_size_cap(monkeypatch):
    big = b"x" * (1 << 16)
    _patch_get(monkeypatch, FakeResponse([big] * 5))
    with pytest.raises(RuntimeError, match="taille"):
        url.download_url("https://example.com/x.csv", "csv", max_bytes=1 << 17)


def test_download_url_rejects_content_length_over_cap(monkeypatch):
    _patch_get(monkeypatch, FakeResponse([b"x"], content_length=10 ** 9))
    with pytest.raises(RuntimeError, match="taille"):
        url.download_url("https://example.com/x.csv", "csv", max_bytes=1 << 20)
