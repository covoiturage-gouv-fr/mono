import os
import time
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
  total = len(tables)
  t_start = time.monotonic()
  imported = 0
  total_rows = 0

  try:
    for i, t in enumerate(tables, 1):
      name = t["name"]
      filename = t.get("filename", name)
      ext = t.get("ext", "parquet")
      geo_layer = t.get("geo_layer")
      select = t.get("select")
      step = f"[{i}/{total}] {name}"

      if "url" in t:
        path = get_last_url(t["url"]["api"], t["url"]["path"])
        key = None
      else:
        key, path = s3_path(filename, ext, bucket, folder)
        if not s3_exists(bucket, key, s3):
          print(f"❌ {step} — fichier manquant : {path}")
          continue

      if name in existing_tables:
        if not overwrite:
          print(f"⏭️  {step} — déjà présente dans {schema}, skip")
          continue
        print(f"ℹ️  {step} — suppression pour overwrite")
        drop_table(conn, schema, name)

      if key:  # tout fichier S3 est rapatrié via boto3 : httpfs DuckDB casse après du travail spatial
        if key not in local_cache:
          print(f"▶️  {step} — téléchargement de {path}...")
          local_cache[key] = s3_download(bucket, key, ext, s3)
        else:
          print(f"▶️  {step} — fichier déjà en cache local")
        path = local_cache[key]

      total_rows += import_table(table=name, schema=schema, path=path, ext=ext, geo_layer=geo_layer, select=select, conn=conn)
      imported += 1

  finally:
    for tmp_path in local_cache.values():
      if os.path.exists(tmp_path):
        os.unlink(tmp_path)

  conn.close()

  elapsed = time.monotonic() - t_start
  rows_fmt = f"{total_rows:_}".replace("_", " ")  # séparateur de milliers à la française
  print(f"🏁 {schema} : {imported}/{total} tables importées, {rows_fmt} lignes en {elapsed:.1f}s")

if __name__ == "__main__":
  app()