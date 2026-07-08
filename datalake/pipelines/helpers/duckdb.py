import os
import duckdb
from pathlib import Path
from typing import Optional

# Fichier DuckDB = cache scratch S3→Postgres, doit vivre dans un répertoire
# inscriptible (l'image monte /data en lecture seule) ; surchargeable par volume.
DEFAULT_DUCKDB_PATH = "/tmp/datalake/db.duckdb"


def _resolve_db_path(db_file: Optional[str] = None) -> str:
    return db_file or os.getenv("DUCKDB_PATH", DEFAULT_DUCKDB_PATH)


def duckdb_client(db_file: Optional[str] = None) -> duckdb.DuckDBPyConnection:
    db_file = _resolve_db_path(db_file)
    Path(db_file).parent.mkdir(parents=True, exist_ok=True)

    conn = duckdb.connect(db_file)
    conn.execute("INSTALL httpfs; LOAD httpfs;")
    conn.execute("INSTALL postgres; LOAD postgres;")
    conn.execute("INSTALL spatial; LOAD spatial;")

    attach_postgres(conn)
    configure_s3(conn)

    return conn


def attach_postgres(conn: duckdb.DuckDBPyConnection):
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


def configure_s3(conn: duckdb.DuckDBPyConnection):
    conn.execute(f"""
    SET s3_endpoint='{os.getenv("S3_ENDPOINT")}';
    SET s3_access_key_id='{os.getenv("S3_ACCESS_KEY")}';
    SET s3_secret_access_key='{os.getenv("S3_SECRET_KEY")}';
    """)