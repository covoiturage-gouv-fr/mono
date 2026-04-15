import os
import duckdb
from pathlib import Path
from typing import Optional


def duckdb_client(db_file: Optional[str] = None) -> duckdb.DuckDBPyConnection:
    db_file = db_file or "./db/db.duckdb"
    Path(db_file).parent.mkdir(parents=True, exist_ok=True)

    conn = duckdb.connect(db_file)
    conn.execute("INSTALL httpfs; LOAD httpfs;")
    conn.execute("INSTALL postgres; LOAD postgres;")
    conn.execute("INSTALL spatial; LOAD spatial;")

    conn.execute(f"""
    CREATE OR REPLACE SECRET pg_secret (
      TYPE postgres,
      HOST '{os.getenv("DBT_HOST")}',
      PORT {os.getenv("DBT_PORT")},
      USER '{os.getenv("DBT_USER")}',
      PASSWORD '{os.getenv("DBT_PASSWORD")}',
      DATABASE '{os.getenv("DBT_DBNAME")}'
    );
    """)

    conn.execute("""
    ATTACH '' AS pg (
      TYPE postgres,
      SECRET pg_secret
    );
    """)

    conn.execute(f"""
    SET s3_endpoint='{os.getenv("S3_ENDPOINT")}';
    SET s3_access_key_id='{os.getenv("S3_ACCESS_KEY")}';
    SET s3_secret_access_key='{os.getenv("S3_SECRET_KEY")}';
    """)
    return conn


def create_schema(conn: duckdb.DuckDBPyConnection, schema: str):
    conn.execute(f"CREATE SCHEMA IF NOT EXISTS pg.{schema};")


def get_existing_tables(schema: str, conn: Optional[duckdb.DuckDBPyConnection] = None) -> set[str]:
    """Retourne les tables existantes dans un schéma Postgres."""
    _conn = conn or duckdb_client()
    rows = _conn.execute(f"""
        SELECT table_name
        FROM pg.information_schema.tables
        WHERE table_schema = '{schema}';
    """).fetchall()
    if not conn:
        _conn.close()
    return {r[0] for r in rows}