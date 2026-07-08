import pytest

from pipelines.helpers import pg


def test_ident_quotes_and_escapes():
    assert pg._ident("aom") == '"aom"'


def test_check_type_accepts_known_types():
    for typ in ["varchar", "TEXT", "bigint", "double precision", "geometry", "varchar(10)", "numeric(12, 2)"]:
        pg._check_type(typ)  # ne lève pas


def test_check_type_rejects_injection_and_unknown():
    with pytest.raises(ValueError, match="type"):
        pg._check_type("text); DROP SCHEMA zone_raw CASCADE; --")
    with pytest.raises(ValueError, match="type"):
        pg._check_type("jsonb")  # hors allowlist


def test_load_csv_columns_rejects_untrusted_type(tmp_path):
    csv = tmp_path / "x.csv"
    csv.write_text("a\nx\n")
    conn = FakePgConn()
    with pytest.raises(ValueError, match="type"):
        pg.load_csv(conn, "zone_raw", "t", str(csv),
                    columns=[["a", "text); DROP TABLE u; --"]])


def test_load_csv_select_rejects_untrusted_type(tmp_path):
    csv = tmp_path / "x.csv"
    csv.write_text("MOD\n1\n")
    conn = FakePgConn()
    with pytest.raises(ValueError, match="type"):
        pg.load_csv(conn, "zone_raw", "t", str(csv),
                    select=[["MOD", "integer); DROP TABLE u; --", "mod"]])
    assert pg._ident("Mise à jour") == '"Mise à jour"'
    assert pg._ident('a"b') == '"a""b"'


class FakeCopy:
    def __init__(self, calls):
        self.calls = calls

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def write(self, data):
        self.calls.append(("copy_write", len(data)))


class FakeCursor:
    def __init__(self, calls):
        self.calls = calls

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def copy(self, sql):
        self.calls.append(sql)
        return FakeCopy(self.calls)


class FakePgConn:
    def __init__(self):
        self.calls = []

    def execute(self, sql, params=None):
        self.calls.append(sql)
        return self

    def fetchone(self):
        return (3,)

    def cursor(self):
        return FakeCursor(self.calls)


def _sql(conn):
    return " || ".join(c for c in conn.calls if isinstance(c, str))


def test_load_csv_columns_creates_typed_table_and_copies_with_force_null(tmp_path):
    csv = tmp_path / "x.csv"
    csv.write_text("a,n\nx,1\n")
    conn = FakePgConn()
    n = pg.load_csv(conn, "zone_raw", "t", str(csv),
                    columns=[["a", "varchar"], ["n", "bigint"]])
    joined = _sql(conn)
    assert 'CREATE TABLE "zone_raw"."t" ("a" varchar, "n" bigint)' in joined
    assert 'FORCE_NULL ("a", "n")' in joined  # champ vide -> NULL
    assert "LIMIT" not in joined
    assert n == 3


def test_load_csv_select_uses_text_staging_and_casts(tmp_path):
    csv = tmp_path / "i.csv"
    csv.write_text("MOD,DATE_EFF\n32,2025-01-01\n")
    conn = FakePgConn()
    pg.load_csv(conn, "zone_raw", "insee", str(csv),
                select=[["MOD", "integer", "mod"], ["DATE_EFF", "date", "date_eff"]])
    joined = _sql(conn)
    assert 'CREATE TEMP TABLE "_staging_insee" ("MOD" text, "DATE_EFF" text)' in joined
    assert 'CAST(NULLIF("MOD", \'\') AS integer) AS "mod"' in joined
    assert 'CAST(NULLIF("DATE_EFF", \'\') AS date) AS "date_eff"' in joined
