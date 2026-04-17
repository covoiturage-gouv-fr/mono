import typer
from typing import Optional
from dotenv import load_dotenv
from pipelines.helpers.config import load_config
from pipelines.helpers.duckdb import duckdb_client
from pipelines.helpers.sql import get_existing_tables, drop_table
from pipelines.helpers.s3 import s3_client, s3_exists, s3_path
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

  for t in tables:
    name = t["name"]
    filename = t.get("filename", name)
    ext = t.get("ext", "parquet")
    geo_layer = t.get("geo_layer")
    select = t.get("select")

    if "url" in t:
      path = get_last_url(t["url"]["api"], t["url"]["path"])
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

    import_table(table=name, schema=schema, path=path, ext=ext, geo_layer=geo_layer, select=select, conn=conn)

  conn.close()


if __name__ == "__main__":
  app()