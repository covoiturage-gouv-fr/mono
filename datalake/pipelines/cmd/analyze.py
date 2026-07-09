"""ANALYZE des foreign tables (FDW) du datalake.

Autovacuum n'analyse JAMAIS les foreign tables : sans stats locales, `reltuples = -1`
et le planner estime mal les fragments de requête non délégués au serveur distant.
À lancer avant un backfill lourd.

Remplace un ancien `psql \\gexec` : `psql` n'est pas présent dans l'image du pod. On passe
par psycopg (déjà une dépendance) — aucune dépendance à un binaire externe.
"""

import time

import psycopg
import typer
from dotenv import load_dotenv
from psycopg import sql

from pipelines.helpers.pg import pg_conninfo

load_dotenv()
app = typer.Typer()


def foreign_tables(conn, schema: str) -> list[str]:
    """Noms des foreign tables du schéma, triés (ordre déterministe)."""
    rows = conn.execute(
        "SELECT foreign_table_name FROM information_schema.foreign_tables "
        "WHERE foreign_table_schema = %s ORDER BY foreign_table_name",
        (schema,),
    ).fetchall()
    return [r[0] for r in rows]


@app.command()
def analyze(schema: str = "dlk_import"):
    with psycopg.connect(pg_conninfo(), autocommit=True) as conn:
        tables = foreign_tables(conn, schema)
        if not tables:
            print(f"⚠️  aucune foreign table dans « {schema} » — rien à analyser")
            return
        print(f"ANALYZE de {len(tables)} foreign tables ({schema})…")
        for i, table in enumerate(tables, 1):
            start = time.perf_counter()
            conn.execute(sql.SQL("ANALYZE {}").format(sql.Identifier(schema, table)))
            print(f"  [{i}/{len(tables)}] {schema}.{table} ({time.perf_counter() - start:.1f}s)")
        print("✅ ANALYZE terminé")


if __name__ == "__main__":
    app()
