import os
import typer
from typing import Optional
from dotenv import load_dotenv
from pipelines.helpers.config import load_config
from pipelines.helpers.duckdb import duckdb_client
from pipelines.helpers.sql import get_existing_tables, drop_table
from pipelines.helpers.s3 import s3_client, s3_exists, s3_path, s3_download
from pipelines.helpers.url import get_last_url
from pipelines.tasks.db_sync import import_table

load_dotenv()
app = typer.Typer()


@app.command()
def seed(
  config: str,
  schema: str,
  bucket: Optional[str] = typer.Option(default=None, envvar="S3_BUCKET"),
  folder: Optional[str] = None,
  overwrite: bool = False,
):
  tables = load_config(config)
  conn = duckdb_client()
  s3 = s3_client()
  existing_tables = get_existing_tables(conn, schema)

  local_cache: dict[str, str] = {}

  try:
    for t in tables:
      name = t["name"]
      filename = t.get("filename", name)
      ext = t.get("ext", "parquet")
      geo_layer = t.get("geo_layer")
      select = t.get("select")
      chunk_size = t.get("chunk_size")

      if "url" in t:
        path = get_last_url(t["url"]["api"], t["url"]["path"])
        key = None
      else:
        key, path = s3_path(filename, ext, bucket, folder)
        if not s3_exists(bucket, key, s3):
          print(f"❌ [{filename}] Fichier manquant : {path}")
          continue

      if name in existing_tables:
        if not overwrite:
          print(f"⏭️  [{name}] Déjà présente dans {schema}, skipping.")
          continue
        print(f"ℹ️  [{name}] Suppression pour overwrite.")
        drop_table(conn, schema, name)

      if chunk_size and key:
        if key not in local_cache:
          print(f"▶️  [{name}] Téléchargement de {path}...")
          local_cache[key] = s3_download(bucket, key, ext, s3)
        else:
          print(f"▶️  [{name}] -> Fichier déjà en cache local")
        path = local_cache[key]

      import_table(table=name, schema=schema, path=path, ext=ext, geo_layer=geo_layer, select=select, chunk_size=chunk_size, conn=conn)

  finally:
    for tmp_path in local_cache.values():
      if os.path.exists(tmp_path):
        os.unlink(tmp_path)

  conn.close()

if __name__ == "__main__":
  app()