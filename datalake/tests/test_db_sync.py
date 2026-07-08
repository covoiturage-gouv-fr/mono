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
    assert any("DROP TABLE IF EXISTS pg.s.t" in c for c in conn.calls)  # pas de table à moitié remplie
