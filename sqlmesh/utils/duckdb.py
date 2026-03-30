import os
import duckdb
from sqlmesh.core.context import Context
from pathlib import Path
from typing import Optional, Tuple
from utils.s3 import get_s3_client, s3_file_exists, build_s3_path

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
    
    # Récupérer la connection SQLMesh Postgres
    context = Context()
    pg_gateway = context.config.gateways["postgres"]
    conn_info = pg_gateway.connection
    catalog_name = conn_info.database or "pg_catalog"
    
    # Créer le secret Postgres si nécessaire
    conn.execute(f"""
    CREATE SECRET (
      TYPE postgres,
      HOST '{conn_info.host}',
      PORT {conn_info.port},
      USER '{conn_info.user}',
      PASSWORD '{conn_info.password}',
      DATABASE '{conn_info.database}'
    );
    """)
    conn.execute(f"""
      ATTACH '' AS postgres_db (TYPE postgres);
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
  conn.execute(f"CREATE SCHEMA IF NOT EXISTS postgres_db.{schema};")

def get_existing_tables(conn: duckdb.DuckDBPyConnection, schema: str, view: bool = False) -> set[str]:
  """
  Récupère la liste des tables existantes dans un schema Postgres avec DuckDB.
  """
  if view: TABLE_TYPE = "views"
  else: TABLE_TYPE = "tables"
  rows = conn.execute(f"""
      SELECT table_name
      FROM postgres_db.information_schema.{TABLE_TYPE}
      WHERE table_schema = '{schema}';
  """).fetchall()
  return {t[0] for t in rows}

def export_tables(
  conn: duckdb.DuckDBPyConnection,
  tables: Tuple[str, ...],
  schema: Optional[str] = None,
  bucket: Optional[str] = None,
  folder: Optional[str] = None,
  overwrite: bool = False,
  view: bool = False
):
  schema = schema or 'raw_zone'
  bucket = bucket or os.getenv("S3_BUCKET")
  folder = folder or 'exports'

  if not tables:
    print("⚠️ Aucune table fourni")
    return
  # Client S3
  s3_client = get_s3_client()
  # Vérifier que les vues existent
  existing_tables = get_existing_tables(conn, schema, view)
 
  for t in tables:
      if t not in existing_tables:
          print(f"⚠️ {view and 'Vue' or 'Table'} {t} inexistante dans {schema}, skipping.")
          continue
      
      s3_key, s3_path = build_s3_path(bucket, folder, t, 'parquet')
       # Vérifier si le fichier existe déjà et gérer l'overwrite
      if not overwrite and s3_file_exists(bucket, s3_key, s3_client):
          print(f"ℹ️ Fichier {s3_path} existe déjà sur {bucket}, skipping.")
          continue
      print(f"▶️ Export de {schema}.{t} → {s3_path}")
      conn.execute(f"COPY (SELECT * FROM postgres_db.{schema}.{t}) TO '{s3_path}' (FORMAT PARQUET);")
      print(f"✅ Export terminé : {s3_path}")

def import_tables(
    conn: duckdb.DuckDBPyConnection,
    tables: Tuple[str, ...],
    schema: Optional[str] = None,
    bucket: Optional[str] = None,
    folder: Optional[str] = None,
    overwrite: bool = False,
):
    schema = schema or 'archive_zone'
    bucket = bucket or os.getenv("S3_BUCKET")
    folder = folder or 'exports'

    if not tables:
        print("⚠️ Aucune table fournie")
        return
    # Client S3
    s3_client = get_s3_client()
    # Tables existantes côté Postgres
    existing_tables = get_existing_tables(conn, schema)

    for t in tables:
        s3_key, s3_path = build_s3_path(bucket, folder, t, 'parquet')
        # Vérifier existence S3
        if not s3_file_exists(bucket, s3_key, s3_client):
            print(f"⚠️ Fichier {s3_path} inexistant sur {bucket}, skipping.")
            continue

        # Gestion overwrite
        if t in existing_tables:
            if overwrite:
                print(f"ℹ️ Table {schema}.{t} existante, suppression pour overwrite.")
                conn.execute(f"DROP TABLE postgres_db.{schema}.{t};")
            else:
                print(f"ℹ️ Table {schema}.{t} existe déjà, skipping.")
                continue
        # Créer le schema si nécessaire
        create_schema(conn, schema)
        
        print(f"▶️ Import de {s3_path} → {schema}.{t}")

        conn.execute(f"""
            CREATE TABLE postgres_db.{schema}.{t} AS
            SELECT * FROM read_parquet('{s3_path}');
        """)
        print(f"✅ Import terminé : {schema}.{t}")




