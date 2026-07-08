import pytest
from pipelines.helpers.retry import retry


def test_retry_returns_on_first_success(monkeypatch):
    monkeypatch.setattr("time.sleep", lambda s: None)
    calls = {"n": 0}

    def fn():
        calls["n"] += 1
        return "ok"

    assert retry(fn) == "ok"
    assert calls["n"] == 1


def test_retry_succeeds_after_transient_failures(monkeypatch):
    monkeypatch.setattr("time.sleep", lambda s: None)
    calls = {"n": 0}

    def fn():
        calls["n"] += 1
        if calls["n"] < 3:
            raise ConnectionError("transient")
        return "ok"

    assert retry(fn, attempts=4) == "ok"
    assert calls["n"] == 3


def test_retry_reraises_after_exhausting_attempts(monkeypatch):
    monkeypatch.setattr("time.sleep", lambda s: None)
    calls = {"n": 0}

    def fn():
        calls["n"] += 1
        raise ValueError("boom")

    with pytest.raises(ValueError):
        retry(fn, attempts=3)
    assert calls["n"] == 3  # 3 essais puis abandon
