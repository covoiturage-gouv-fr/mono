import os
from helpers.s3 import s3_client, s3_exists, s3_path
from helpers.duckdb import duckdb_client, create_schema
from typing import Optional

def import_table(
    table: str,
    schema: Optional[str] = None,
    bucket: Optional[str] = None,
    folder: Optional[str] = None,
    ext: str = 'parquet',
    geo_layer: Optional[str] = None,
    overwrite: bool = False,
):
    conn = duckdb_client()
    schema = schema or 'archive_zone'
    bucket = bucket or os.getenv("S3_BUCKET")
    folder = folder or 'exports'

    if not table:
        print("⚠️ Aucune table fournie")
        return
    # Client S3
    s3 = s3_client()
    key, path = s3_path(bucket, folder, table, ext)
    # Vérifier existence S3
    if not s3_exists(bucket, key, s3):
      print(f"⚠️ Fichier {path} inexistant sur {bucket}")
      return
    
    if overwrite:
      print(f"ℹ️ Table {schema}.{table} existante, suppression pour overwrite.")
      conn.execute(f"DROP TABLE IF EXISTS pg.{schema}.{table};")
    
    # Créer le schema si nécessaire
    create_schema(conn, schema)
        
    print(f"▶️ Import de {path} → {schema}.{table}")

    if ext in ("gpkg", "geojson", "shp"):
      if geo_layer:
        sql = f"""
        CREATE TABLE pg.{schema}.{table} AS
        SELECT * FROM st_read('{path}', layer='{geo_layer}');
        """
      else:
        sql = f"""
        CREATE TABLE pg.{schema}.{table} AS
        SELECT * FROM st_read('{path}');
        """
    else:
      sql = f"""
      CREATE TABLE pg.{schema}.{table} AS
      SELECT * FROM read_parquet('{path}');
      """
    conn.execute(sql)
    print(f"✅ Import terminé : {schema}.{table}")

def export_table(
  table: str,
  schema: Optional[str] = None,
  bucket: Optional[str] = None,
  folder: Optional[str] = None,
  overwrite: bool = False,
):
  conn = duckdb_client()
  schema = schema or 'raw_zone'
  bucket = bucket or os.getenv("S3_BUCKET")
  folder = folder or 'exports'

  if not table:
    print("⚠️ Aucune table fourni")
    return
  # Client S3
  s3 = s3_client()
  key, path = s3_path(bucket, folder, table, 'parquet')
  # Vérifier si le fichier existe déjà et gérer l'overwrite
  if not overwrite and s3_exists(bucket, key, s3):
    print(f"ℹ️ Fichier {path} existe déjà sur {bucket}, skipping.")
    return     
  print(f"▶️ Export de {schema}.{table} → {path}")
  conn.execute(f"COPY (SELECT * FROM pg.{schema}.{table}) TO '{path}' (FORMAT PARQUET);")
  print(f"✅ Export terminé : {path}")






