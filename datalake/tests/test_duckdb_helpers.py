from pipelines.helpers.duckdb import DEFAULT_DUCKDB_PATH, _resolve_db_path


def test_resolve_db_path_prefers_explicit_arg(monkeypatch):
    monkeypatch.setenv("DUCKDB_PATH", "/vol/cache.duckdb")
    assert _resolve_db_path("/custom/x.duckdb") == "/custom/x.duckdb"


def test_resolve_db_path_uses_env(monkeypatch):
    monkeypatch.setenv("DUCKDB_PATH", "/vol/cache.duckdb")
    assert _resolve_db_path() == "/vol/cache.duckdb"


def test_resolve_db_path_defaults_to_writable_tmp(monkeypatch):
    monkeypatch.delenv("DUCKDB_PATH", raising=False)
    assert _resolve_db_path() == DEFAULT_DUCKDB_PATH
    assert DEFAULT_DUCKDB_PATH.startswith("/tmp/")
