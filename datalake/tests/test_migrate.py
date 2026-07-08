from unittest.mock import MagicMock

import pytest

from pipelines.cmd.migrate import _apply, discover, run_migrations, select_pending
from pipelines.helpers.pg import pg_conninfo


def test_apply_rejects_bad_version():
    # version is a repo filename stem; a non-allowlisted stem must raise before any SQL
    conn = MagicMock()
    with pytest.raises(ValueError):
        _apply(conn, "0001_bad-name", "SELECT 1;", "public.schema_migrations")
    conn.pgconn.exec_.assert_not_called()


def test_discover_is_sorted(tmp_path):
    (tmp_path / "0002_b.sql").write_text("SELECT 1;")
    (tmp_path / "0001_a.sql").write_text("SELECT 1;")
    assert [f.stem for f in discover(tmp_path)] == ["0001_a", "0002_b"]


def test_select_pending_skips_applied(tmp_path):
    (tmp_path / "0001_a.sql").write_text("SELECT 1;")
    (tmp_path / "0002_b.sql").write_text("SELECT 1;")
    files = discover(tmp_path)
    assert [f.stem for f in select_pending(files, set())] == ["0001_a", "0002_b"]
    assert [f.stem for f in select_pending(files, {"0001_a"})] == ["0002_b"]


def _db_available() -> bool:
    import psycopg
    try:
        with psycopg.connect(pg_conninfo(), connect_timeout=2):
            return True
    except Exception:
        return False


needs_db = pytest.mark.skipif(not _db_available(), reason="no local Postgres reachable")


@needs_db
def test_run_migrations_applies_multistatement_and_is_idempotent(tmp_path):
    import psycopg

    ledger = "public._mig_test_ledger"
    # a multi-statement migration incl. a $$-quoted function body
    (tmp_path / "0001_setup.sql").write_text(
        "CREATE SCHEMA IF NOT EXISTS _mig_test;\n"
        "CREATE TABLE IF NOT EXISTS _mig_test.t (id int);\n"
        "CREATE OR REPLACE FUNCTION _mig_test.f() RETURNS int LANGUAGE sql AS $$ SELECT 1; $$;\n"
    )

    with psycopg.connect(pg_conninfo(), autocommit=True) as conn:
        conn.execute(f"DROP TABLE IF EXISTS {ledger}")
        conn.execute("DROP SCHEMA IF EXISTS _mig_test CASCADE")
        try:
            applied = run_migrations(conn, tmp_path, ledger=ledger)
            assert applied == ["0001_setup"]
            tables = conn.execute(
                "SELECT count(*) FROM information_schema.tables WHERE table_schema='_mig_test'"
            ).fetchone()[0]
            assert tables == 1
            # re-run is a no-op
            assert run_migrations(conn, tmp_path, ledger=ledger) == []
        finally:
            conn.execute(f"DROP TABLE IF EXISTS {ledger}")
            conn.execute("DROP SCHEMA IF EXISTS _mig_test CASCADE")


@needs_db
def test_failed_migration_rolls_back(tmp_path):
    import psycopg

    ledger = "public._mig_test_ledger2"
    (tmp_path / "0001_ok.sql").write_text("CREATE SCHEMA IF NOT EXISTS _mig_test2;")
    (tmp_path / "0002_bad.sql").write_text("CREATE TABLE _mig_test2.t (id int); SELECT bad_fn();")

    with psycopg.connect(pg_conninfo(), autocommit=True) as conn:
        conn.execute(f"DROP TABLE IF EXISTS {ledger}")
        conn.execute("DROP SCHEMA IF EXISTS _mig_test2 CASCADE")
        try:
            with pytest.raises(Exception):
                run_migrations(conn, tmp_path, ledger=ledger)
            # 0001 committed and recorded; 0002 rolled back, not recorded, table absent
            versions = {r[0] for r in conn.execute(f"SELECT version FROM {ledger}").fetchall()}
            assert versions == {"0001_ok"}
            tbl = conn.execute(
                "SELECT count(*) FROM information_schema.tables WHERE table_schema='_mig_test2'"
            ).fetchone()[0]
            assert tbl == 0
        finally:
            conn.execute(f"DROP TABLE IF EXISTS {ledger}")
            conn.execute("DROP SCHEMA IF EXISTS _mig_test2 CASCADE")
