import os
import time
import typer
from typing import Optional
from dotenv import load_dotenv
from pipelines.helpers.config import load_config
from pipelines.helpers import pg
from pipelines.helpers.checksum import verify_checksum, verify_size
from pipelines.helpers.s3 import s3_client, s3_exists, s3_path, s3_download
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
  skip_checksum: bool = typer.Option(False, help="Autorise les sources sans empreinte (ex. data.gouv) ; celles qui en ont une restent vérifiées."),
):
  tables = load_config(config)
  conn = pg.pg_connect()
  s3 = s3_client()
  existing_tables = pg.existing_tables(conn, schema)

  # SHA256/taille attendus par fichier (committés en config = infalsifiables sans revue de code).
  checksums = {t.get("filename", t["name"]): t["sha256"] for t in tables if t.get("sha256")}
  sizes = {t.get("filename", t["name"]): t["size"] for t in tables if t.get("size")}

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
      columns = t.get("columns")
      step = f"[{i}/{total}] {name}"

      key, src = s3_path(filename, ext, bucket, folder)
      if not s3_exists(bucket, key, s3):
        print(f"❌ {step} — fichier manquant : {src}")
        continue
      expected = checksums.get(filename)  # empreinte committée en config (= lock)
      expected_size = sizes.get(filename)

      if name in existing_tables:
        if not overwrite:
          print(f"⏭️  {step} — déjà présente dans {schema}, skip")
          continue
        print(f"ℹ️  {step} — suppression pour overwrite")
        pg.drop_table(conn, schema, name)

      # Intégrité exigée par défaut : pas d'empreinte = on refuse (--skip-checksum pour outrepasser).
      if not expected and not skip_checksum:
        raise RuntimeError(f"❌ {step} — aucune empreinte pour {filename} ; --skip-checksum pour outrepasser")

      # Le loader lit un fichier local : on rapatrie la source S3 via boto3.
      cache_key = key
      if cache_key not in local_cache:
        print(f"▶️  {step} — téléchargement de {src}...")
        local_path = s3_download(bucket, key, ext, s3)
        if expected:  # empreinte présente : toujours vérifiée, même avec --skip-checksum
          if expected_size:
            verify_size(local_path, expected_size, filename)
          verify_checksum(local_path, expected, filename)
        local_cache[cache_key] = local_path
      else:
        print(f"▶️  {step} — fichier déjà en cache local")
      path = local_cache[cache_key]

      total_rows += import_table(table=name, schema=schema, path=path, ext=ext, geo_layer=geo_layer, select=select, columns=columns, conn=conn)
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