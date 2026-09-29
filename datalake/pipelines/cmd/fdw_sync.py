"""Importe dans dlk_import les vues de dlk_export (API) qui n'y sont pas encore.

L'IMPORT FOREIGN SCHEMA initial est fait une fois par les ops (voir migrations/0000_fdw.sql) :
une vue ajoutée ensuite côté API n'apparaît pas toute seule. Pas une migration : elle tournerait
au déploiement du datalake, pas forcément après celui de l'API, et serait marquée jouée à vide.
À lancer après le déploiement de l'API. Idempotent : n'importe que les tables absentes.
"""

import psycopg
import typer
from dotenv import load_dotenv
from psycopg import sql

from pipelines.cmd.analyze import foreign_tables
from pipelines.helpers.pg import pg_conninfo

load_dotenv()
app = typer.Typer()


def fdw_server(conn) -> str:
  rows = conn.execute(
    "SELECT s.srvname FROM pg_foreign_server s "
    "JOIN pg_foreign_data_wrapper w ON w.oid = s.srvfdw "
    "WHERE w.fdwname = 'postgres_fdw' ORDER BY s.srvname"
  ).fetchall()
  if len(rows) != 1:
    raise RuntimeError(f"❌ un serveur postgres_fdw attendu, trouvé {len(rows)}")
  return rows[0][0]


def import_statement(server: str, remote: str, local: str, existing: list[str]) -> sql.Composed:
  except_clause = (
    sql.SQL(" EXCEPT ({})").format(sql.SQL(", ").join(map(sql.Identifier, existing))) if existing else sql.SQL("")
  )
  return sql.SQL("IMPORT FOREIGN SCHEMA {}{} FROM SERVER {} INTO {}").format(
    sql.Identifier(remote), except_clause, sql.Identifier(server), sql.Identifier(local)
  )


@app.command()
def sync(remote: str = "dlk_export", local: str = "dlk_import"):
  with psycopg.connect(pg_conninfo(), autocommit=True) as conn:
    before = foreign_tables(conn, local)
    conn.execute(import_statement(fdw_server(conn), remote, local, before))
    added = sorted(set(foreign_tables(conn, local)) - set(before))

  if not added:
    print(f"✅ {local} à jour ({len(before)} tables)")
    return
  for table in added:
    print(f"  + {local}.{table}")
  print(f"✅ {len(added)} table(s) importée(s) — lancer `just analyze-sources` pour leurs stats")


if __name__ == "__main__":
  app()
