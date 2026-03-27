import os
import duckdb
from sqlmesh.core.context import Context
from pathlib import Path
from typing import Optional, Tuple


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

def export_tables(
  conn: duckdb.DuckDBPyConnection,
  tables: Tuple[str, ...],
  schema: Optional[str] = None,
  bucket: Optional[str] = None,
  folder: Optional[str] = None
):
  schema = schema or 'raw_zone'
  bucket = bucket or os.getenv("S3_BUCKET")
  folder = folder or 'exports'

  if not tables:
    print("⚠️ Aucune table fourni")
    return
  # Vérifier que les tables existent
  existing_tables = conn.execute(f"""
      SELECT table_name
      FROM postgres_db.information_schema.views
      WHERE table_schema = '{schema}';
  """).fetchall()
  existing_tables = {t[0] for t in existing_tables}
  
  for t in tables:
      if t not in existing_tables:
          print(f"⚠️ Vue {t} inexistante dans {schema}, skipping.")
          continue
      s3_path = f"s3://{bucket}/{folder}/{t}.parquet"
      print(f"▶️ Export de {schema}.{t} → {s3_path}")
      conn.execute(f"COPY (SELECT * FROM postgres_db.{schema}.{t}) TO '{s3_path}' (FORMAT PARQUET);")
      print(f"✅ Export terminé : {s3_path}")


conn = duckdb_client()
TABLES = [
  'aires_covoiturage'
]
export_tables(conn, TABLES, folder='test')

