import os
import duckdb
from pathlib import Path
from typing import Optional


def duckdb_client(
    db_file: Optional[str] = None,
) -> duckdb.DuckDBPyConnection:
    """
    Crée et retourne un client DuckDB configuré avec :
      - Base persistante
      - Extensions httpfs & spatial
      - Catalog Postgres depuis SQLMesh
      - Connexion S3 pour export Parquet

    Args:
        db_file: Chemin vers le fichier DuckDB. Par défaut './db/db.duckdb'
        s3_bucket: Bucket S3 pour les exports. Sinon prend S3_BUCKET env
        export_prefix: Préfixe pour les fichiers exportés sur S3

    Returns:
        duckdb.DuckDBPyConnection : client prêt à l'emploi
    """
    db_file = db_file or "./db/db.duckdb"

    Path(db_file).parent.mkdir(parents=True, exist_ok=True)
    
    # Connexion DuckDB
    conn = duckdb.connect(db_file)
  
    # Extensions nécessaires
    conn.execute("INSTALL httpfs; LOAD httpfs;")
    conn.execute("INSTALL postgres; LOAD postgres;")
    conn.execute("INSTALL spatial; LOAD spatial;")
    
    # Créer le secret Postgres si nécessaire
    conn.execute(f"""
    CREATE SECRET (
      TYPE postgres,
      HOST '{os.getenv("DBT_HOST")}',
      PORT {os.getenv("DBT_PORT")},
      USER '{os.getenv("DBT_USER")}',
      PASSWORD '{os.getenv("DBT_PASSWORD")}',
      DATABASE '{os.getenv("DBT_DBNAME")}'
    );
    """)
    conn.execute(f"""
      ATTACH '' AS pg (TYPE postgres);
      """  
    )
    # Configurer S3 pour DuckDB httpfs
    conn.execute(f"""
    SET s3_endpoint='{os.getenv("S3_ENDPOINT")}';
    SET s3_access_key_id='{os.getenv("S3_ACCESS_KEY")}';
    SET s3_secret_access_key='{os.getenv("S3_SECRET_KEY")}';
    """)    
    return conn

def create_schema(conn: duckdb.DuckDBPyConnection, schema: str):
  """
  Crée le schema sur la base Postgres si nécessaire avec DuckDB.
  """
  conn.execute(f"CREATE SCHEMA IF NOT EXISTS pg.{schema};")

def get_existing_tables(schema: str, view: bool = False) -> set[str]:
  """
  Récupère la liste des tables existantes dans un schema Postgres avec DuckDB.
  """
  conn = duckdb_client()
  if view: TABLE_TYPE = "views"
  else: TABLE_TYPE = "tables"
  rows = conn.execute(f"""
      SELECT table_name
      FROM pg.information_schema.{TABLE_TYPE}
      WHERE table_schema = '{schema}';
  """).fetchall()
  conn.close()
  return {t[0] for t in rows}

