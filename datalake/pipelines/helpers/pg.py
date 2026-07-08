import csv
import os
import re
from typing import Optional

import psycopg

# Types Postgres autorisés dans le DDL de seed. Le champ `type` de la config n'est pas
# paramétrable en SQL (il est interpolé brut) : cette allowlist est le seul rempart contre
# une injection via un type forgé (CWE-89), même si la config est committée et revue.
_ALLOWED_TYPES = {
    "varchar", "text", "char", "bpchar",
    "smallint", "integer", "int", "bigint",
    "numeric", "decimal", "real", "double precision",
    "boolean",
    "date", "time", "timestamp", "timestamptz",
    "geometry", "geography",
}
# base alpha (+ espaces pour « double precision »), longueur/précision optionnelle « (n) » / « (n, m) ».
_TYPE_RE = re.compile(r"^[a-z][a-z ]*(\(\s*\d+\s*(,\s*\d+\s*)?\))?$")


def pg_conninfo() -> str:
    return (
        f"host={os.getenv('DBT_HOST')} port={os.getenv('DBT_PORT')} "
        f"user={os.getenv('DBT_USER')} password={os.getenv('DBT_PASSWORD')} "
        f"dbname={os.getenv('DBT_DBNAME')}"
    )


def pg_connect():
    conn = psycopg.connect(pg_conninfo(), autocommit=True)
    conn.execute("SET datestyle = 'ISO, DMY'")  # sources FR en JJ/MM/AAAA + ISO
    return conn


def _ident(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def _check_type(typ: str) -> str:
    """Valide un type de colonne avant de l'interpoler dans le DDL (non paramétrable). Lève sinon."""
    norm = typ.strip().lower()
    base = re.sub(r"\s*\(.*\)$", "", norm).strip()
    if not _TYPE_RE.match(norm) or base not in _ALLOWED_TYPES:
        raise ValueError(f"❌ type de colonne non autorisé : {typ!r}")
    return typ


def _qualified(schema: str, table: str) -> str:
    return f"{_ident(schema)}.{_ident(table)}"


def create_schema(conn, schema: str) -> None:
    conn.execute(f"CREATE SCHEMA IF NOT EXISTS {_ident(schema)}")


def existing_tables(conn, schema: str) -> set[str]:
    rows = conn.execute(
        "SELECT table_name FROM information_schema.tables WHERE table_schema = %s",
        (schema,),
    ).fetchall()
    return {r[0] for r in rows}


def drop_table(conn, schema: str, table: str) -> None:
    conn.execute(f"DROP TABLE IF EXISTS {_qualified(schema, table)} CASCADE")


def count_rows(conn, schema: str, table: str) -> int:
    return conn.execute(f"SELECT count(*) FROM {_qualified(schema, table)}").fetchone()[0]


def _copy_file(conn, copy_sql: str, path: str) -> None:
    with open(path, "rb") as f, conn.cursor() as cur, cur.copy(copy_sql) as cp:
        while chunk := f.read(1 << 16):
            cp.write(chunk)


def load_csv(conn, schema: str, table: str, path: str,
             columns: Optional[list] = None, select: Optional[list] = None) -> int:
    """Charge un CSV via COPY natif Postgres. `columns` = [[nom, type], ...] (COPY typé direct) ;
    `select` = [[source, type, cible], ...] (sous-ensemble/renommage via table de staging texte)."""
    if select:
        return _load_csv_transform(conn, schema, table, path, select)

    coldefs = ", ".join(f"{_ident(n)} {_check_type(t)}" for n, t in columns)
    collist = ", ".join(_ident(n) for n, _ in columns)
    drop_table(conn, schema, table)
    conn.execute(f"CREATE TABLE {_qualified(schema, table)} ({coldefs})")
    # FORCE_NULL : un champ vide (même quoté) devient NULL, comme l'inférence CSV de DuckDB.
    _copy_file(
        conn,
        f"COPY {_qualified(schema, table)} ({collist}) "
        f"FROM STDIN WITH (FORMAT csv, HEADER true, FORCE_NULL ({collist}))",
        path,
    )
    return count_rows(conn, schema, table)


def _load_csv_transform(conn, schema: str, table: str, path: str, select: list) -> int:
    with open(path, newline="") as f:
        header = next(csv.reader(f))
    staging = f"_staging_{table}"
    conn.execute(f"DROP TABLE IF EXISTS {_ident(staging)}")
    conn.execute(f"CREATE TEMP TABLE {_ident(staging)} (" + ", ".join(f"{_ident(h)} text" for h in header) + ")")
    _copy_file(conn, f"COPY {_ident(staging)} FROM STDIN WITH (FORMAT csv, HEADER true)", path)

    proj = ", ".join(
        f"CAST(NULLIF({_ident(src)}, '') AS {_check_type(typ)}) AS {_ident(tgt)}" for src, typ, tgt in select
    )
    drop_table(conn, schema, table)
    conn.execute(f"CREATE TABLE {_qualified(schema, table)} AS SELECT {proj} FROM {_ident(staging)}")
    conn.execute(f"DROP TABLE {_ident(staging)}")
    return count_rows(conn, schema, table)
