import pytest
from pipelines.tasks import db_sync


def test_build_ogr_sql_renames_attrs_and_keeps_geometry_source():
    select = [
        ["nom_officiel", "varchar", "l_dep"],
        ["code_insee", "varchar", "dep"],
        ["geometrie", "geometry", "geom"],
    ]
    sql = db_sync._build_ogr_sql(select, "departement")
    # géométrie reprise par son nom source (renommée en sortie via -lco GEOMETRY_NAME), pas de "AS geom" ici
    assert sql == 'SELECT nom_officiel AS l_dep, code_insee AS dep, geometrie FROM "departement"'


def test_build_ogr_sql_keeps_plain_string_columns():
    select = ["year", "arr", "l_arr", ["geom", "geometry", "geom"]]
    assert db_sync._build_ogr_sql(select, "full") == 'SELECT year, arr, l_arr, geom FROM "full"'


def test_build_ogr_sql_none_when_no_select():
    assert db_sync._build_ogr_sql(None, "simple") is None


def test_geom_name_uses_geometry_alias_stripped():
    select = [["nom_officiel", "varchar", "l_arr"], ["geometrie", "geometry", " geom"]]
    assert db_sync._geom_name(select) == "geom"


def test_geom_name_defaults_to_geom():
    assert db_sync._geom_name(None) == "geom"
    assert db_sync._geom_name([["a", "varchar", "b"]]) == "geom"


class FakeConn:
    """psycopg minimal : enregistre les requêtes, renvoie un compte fixe."""

    def __init__(self):
        self.calls = []

    def execute(self, sql, params=None):
        self.calls.append(sql)
        return self

    def fetchone(self):
        return (0,)


def test_import_table_drops_partial_table_on_error(monkeypatch):
    conn = FakeConn()
    monkeypatch.setattr(db_sync.pg, "load_csv", lambda *a, **k: (_ for _ in ()).throw(RuntimeError("boom")))
    with pytest.raises(RuntimeError):
        db_sync.import_table(table="t", schema="s", path="/x.csv", ext="csv", columns=[["a", "varchar"]], conn=conn)
    assert any("DROP TABLE IF EXISTS" in c and '"s"."t"' in c for c in conn.calls)  # nettoyage de la table partielle


def test_import_table_rejects_unknown_ext():
    conn = FakeConn()
    with pytest.raises(ValueError):
        db_sync.import_table(table="t", schema="s", path="/x.parquet", ext="parquet", conn=conn)
