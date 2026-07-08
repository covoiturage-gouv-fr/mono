import pytest
from pipelines.tasks import db_sync


class FakeConn:
    """Enregistre les requêtes ; peut échouer sur une sous-chaîne donnée."""

    def __init__(self, fail_on=None):
        self.calls = []
        self.fail_on = fail_on

    def execute(self, sql):
        self.calls.append(sql)
        if self.fail_on and self.fail_on in sql:
            raise RuntimeError("boom")
        return self

    def fetchall(self):
        return []

    def fetchone(self):
        return (42,)  # nombre de lignes renvoyé par un CREATE TABLE AS DuckDB


def test_import_table_reads_source_once():
    conn = FakeConn()
    db_sync.import_table(table="t", schema="s", path="/x.parquet", ext="parquet", conn=conn)
    creates = [c for c in conn.calls if "CREATE TABLE pg.s.t AS" in c]
    joined = " ".join(conn.calls)
    assert len(creates) == 1
    assert "LIMIT" not in joined and "OFFSET" not in joined  # pas de scan O(n²)
    assert "DROP TABLE" not in joined  # rien à nettoyer si succès


def test_import_table_returns_row_count():
    conn = FakeConn()
    count = db_sync.import_table(table="t", schema="s", path="/x.parquet", ext="parquet", conn=conn)
    assert count == 42  # remonté pour le récap de fin de seed


def test_import_table_drops_partial_table_on_error():
    conn = FakeConn(fail_on="CREATE TABLE pg.s.t AS")
    with pytest.raises(RuntimeError):
        db_sync.import_table(table="t", schema="s", path="/x.parquet", ext="parquet", conn=conn)
    # nettoyage via postgres_execute (marche même pour une table créée hors du cache DuckDB par ogr2ogr)
    assert any("DROP TABLE IF EXISTS s.t" in c for c in conn.calls)


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
    # alias " geom" (coquille de la config avec espace) → "geom"
    select = [["nom_officiel", "varchar", "l_arr"], ["geometrie", "geometry", " geom"]]
    assert db_sync._geom_name(select) == "geom"


def test_geom_name_defaults_to_geom():
    assert db_sync._geom_name(None) == "geom"
    assert db_sync._geom_name([["a", "varchar", "b"]]) == "geom"
