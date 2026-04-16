import os
import typer
import json
from pipelines.tasks.external_data import import_table
from pipelines.helpers.duckdb import get_existing_tables, duckdb_client
from pipelines.helpers.s3 import s3_client, s3_exists, s3_path
from typing import Optional, List
from dotenv import load_dotenv

load_dotenv()
app = typer.Typer()

def load_config(path: str):
    with open(path) as f:
        config = json.load(f)
    return config

@app.command()
def seed_external_data(
  config: Optional[str] = 'pipelines/config/external_data_config.json',
  schema: Optional[str] = None,
  bucket: Optional[str] = None,
  folder: Optional[str] = None,
  overwrite: bool = False,
):
  tables = load_config(config) 
  schema = schema or 'dbt_raw'
  bucket = bucket or os.getenv("S3_BUCKET")
  conn = duckdb_client()
  s3 = s3_client()

  existing_tables = get_existing_tables(schema, conn)
  for t in tables:
    name = t["name"]
    filename = t.get("filename", name)
    ext = t.get("ext", "parquet")
    geo_layer = t.get("geo_layer")
    select = t.get("select")
    key, path = s3_path(filename, ext, bucket, folder)
    # Vérification S3
    if not s3_exists(bucket, key, s3):
      print(f"❌ [{filename}] Fichier manquant : {path}")
      continue
    # Vérification table existante
    if name in existing_tables and not overwrite:
      print(f"⏭️  [{name}] Déjà présente dans {schema}, skipping.")
      continue
    # Suppression table existante si overwrite
    if name in existing_tables and overwrite:
      print(f"ℹ️  [{name}] Suppression pour overwrite.")
      conn.execute(f"DROP TABLE IF EXISTS pg.{schema}.{name};")
    
    import_table(table=name, schema=schema, path=path, ext=ext, geo_layer=geo_layer, conn=conn, select=select)
  conn.close()

if __name__ == "__main__":
  app()