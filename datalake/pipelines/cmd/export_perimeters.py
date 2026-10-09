import os
import subprocess
from datetime import datetime, timezone
from typing import Optional

import typer
from dotenv import load_dotenv

from pipelines.helpers import pg
from pipelines.helpers.checksum import hash_file
from pipelines.helpers.s3 import s3_client, s3_upload

load_dotenv()
app = typer.Typer()

# Mêmes colonnes et types que geo.perimeters (prod) : la table remplace geo.perimeters à l'import.
STAGING_SCHEMA = "geo_export"
_COLUMNS = [
  "(ROW_NUMBER() OVER (ORDER BY year, arr))::integer AS id",
  "year::smallint AS year",
  # L'import IGN force le multi (PROMOTE_TO_MULTI) : centroïdes en MultiPoint, voire collections.
  # Les 6 villages détruits de la Meuse (sans chef-lieu) n'ont pas de centroïde IGN.
  "COALESCE(ST_Centroid(centroid), ST_PointOnSurface(ST_CollectionExtract(geom, 3)))::geometry(Point, 4326) AS centroid",
  "ST_Multi(ST_CollectionExtract(geom, 3))::geometry(MultiPolygon, 4326) AS geom",
  "ST_Multi(ST_CollectionExtract(geom_simple, 3))::geometry(MultiPolygon, 4326) AS geom_simple",
  "l_arr::varchar(256) AS l_arr",
  "arr::varchar(5) AS arr",
  "l_com::varchar(256) AS l_com",
  "com::varchar(5) AS com",
  "l_epci::varchar(256) AS l_epci",
  "epci::varchar(9) AS epci",
  "l_dep::varchar(256) AS l_dep",
  "dep::varchar(3) AS dep",
  "l_reg::varchar(256) AS l_reg",
  "reg::varchar(2) AS reg",
  "l_country::varchar(256) AS l_country",
  "country::varchar(5) AS country",
  "l_aom::varchar(256) AS l_aom",
  "aom::varchar(9) AS aom",
  "NULL::varchar(256) AS l_reseau",
  "NULL::integer AS reseau",
  "pop::integer AS pop",
  "surface::real AS surface",
  "valid_from::date AS valid_from",
  "valid_until::date AS valid_until",
]


COM_EVOLUTION_SQL = (
  f"CREATE TABLE {STAGING_SCHEMA}.com_evolution AS "
  "SELECT year::smallint AS year, mod::smallint AS mod, old_com::varchar(5) AS old_com, "
  "new_com::varchar(5) AS new_com, l_mod::varchar AS l_mod FROM zone_trusted.com_evolution"
)


def staging_sql(source: str, years: list[int]) -> str:
  return (
    f"CREATE TABLE {STAGING_SCHEMA}.perimeters AS "
    f"SELECT {', '.join(_COLUMNS)} FROM {source} WHERE year IN ({', '.join(str(int(y)) for y in years)})"
  )


def dump_name(years: list[int], now: datetime) -> str:
  return f"perimeters_{'-'.join(str(int(y)) for y in sorted(years))}.{now.strftime('%Y%m%dT%H%M%SZ')}.pgdump"


def sha256_line(sha: str, name: str) -> str:
  """Contenu du fichier `<dump>.sha256`, au format sha256sum, lu par `geo-import`."""
  return f"{sha}  {name}\n"


def _fmt(n: int) -> str:
  return f"{n:_}".replace("_", " ")


def _pg_env() -> dict:
  # Mot de passe passé par l'environnement libpq (jamais dans argv/ps).
  return {
    **os.environ,
    "PGHOST": os.getenv("DBT_HOST", ""), "PGPORT": os.getenv("DBT_PORT", ""),
    "PGUSER": os.getenv("DBT_USER", ""), "PGPASSWORD": os.getenv("DBT_PASSWORD", ""),
    "PGDATABASE": os.getenv("DBT_DBNAME", ""),
  }


@app.command()
def export(
  year: Optional[list[int]] = typer.Option(default=None, help="Millésime(s) à exporter (répétable)"),
  last: int = typer.Option(default=2, help="Sans --year : nombre de millésimes les plus récents"),
  table: str = "perimeters",
  schema: str = "zone_trusted",
  bucket: Optional[str] = typer.Option(default=None, envvar="S3_BUCKET"),
  folder: str = "geo",
  upload: bool = True,
  out_dir: str = typer.Option(default="tmp/geo", help="Dossier du dump (tmp/ est ignoré par git)"),
):
  """Dump des millésimes de `{schema}.{table}` et de com_evolution au format du schéma geo (prod).

  Par défaut les 2 derniers : geo.perimeters ne garde que ceux-là, le précédent avec son valid_until
  à jour. Passe par `geo_export.perimeters` et `geo_export.com_evolution` (pg_dump ne sait pas dumper
  une requête), supprimées ensuite. Le fichier pg_dump custom et son `.sha256` sont gardés dans `--out-dir`
  (tmp/geo, ignoré par git) et uploadés sur S3.
  Import : `geo-import stage <nom du dump>` dans le pod, `just geo-import <chemin>` en local.
  """
  source = f'"{schema}"."{table}"'
  conn = pg.pg_connect()
  years = year or [
    r[0] for r in conn.execute(f"SELECT DISTINCT year FROM {source} ORDER BY year DESC LIMIT %s", (last,))
  ]
  if not years:
    raise RuntimeError(f"❌ aucun millésime dans {source}")

  perimeters = f"{STAGING_SCHEMA}.perimeters"
  com_evolution = f"{STAGING_SCHEMA}.com_evolution"
  os.makedirs(out_dir, exist_ok=True)
  name = dump_name(years, datetime.now(timezone.utc))
  path = os.path.abspath(os.path.join(out_dir, name))

  pg.create_schema(conn, STAGING_SCHEMA)
  try:
    print(f"▶️  {source} millésimes {sorted(years)} → {perimeters}")
    conn.execute(f"DROP TABLE IF EXISTS {perimeters}")
    conn.execute(staging_sql(source, years))
    counts = dict(conn.execute(f"SELECT year, count(*) FROM {perimeters} GROUP BY year").fetchall())
    missing_geo = conn.execute(
      f"SELECT count(*) FROM {perimeters} WHERE geom IS NULL OR geom_simple IS NULL OR centroid IS NULL"
    ).fetchone()[0]
    if set(counts) != set(years) or missing_geo:
      raise RuntimeError(f"❌ millésimes {counts} (attendus {sorted(years)}), {missing_geo} lignes sans géométrie")
    total = sum(counts.values())

    print(f"▶️  zone_trusted.com_evolution → {com_evolution}")
    conn.execute(f"DROP TABLE IF EXISTS {com_evolution}")
    conn.execute(COM_EVOLUTION_SQL)
    if conn.execute(f"SELECT count(*) FROM {com_evolution}").fetchone()[0] == 0:
      raise RuntimeError("❌ zone_trusted.com_evolution vide")

    print(f"▶️  pg_dump {perimeters}, {com_evolution} → {path}")
    proc = subprocess.run(
      ["pg_dump", "-Fc", "--no-owner", "--no-acl", "-t", perimeters, "-t", com_evolution, "-f", path],
      env=_pg_env(), capture_output=True, text=True,
    )
    if proc.returncode != 0:
      if os.path.exists(path):
        os.unlink(path)
      raise RuntimeError(f"pg_dump a échoué ({proc.returncode}) : {proc.stderr.strip()[:500]}")
  finally:
    conn.execute(f"DROP TABLE IF EXISTS {perimeters}")
    conn.execute(f"DROP TABLE IF EXISTS {com_evolution}")
    conn.close()

  sha = hash_file(path)
  with open(f"{path}.sha256", "w") as f:
    f.write(sha256_line(sha, name))
  print(f"✅ {path} — {_fmt(total)} lignes, {_fmt(os.path.getsize(path))} octets")

  if upload:
    # geo-import lit le dump et son .sha256 sous geo/ : garder --folder à sa valeur par défaut.
    key = f"{folder}/{name}" if folder else name
    client = s3_client()
    print(f"▶️  Upload s3://{bucket}/{key} (+ .sha256)")
    s3_upload(bucket, key, path, client=client)
    s3_upload(bucket, f"{key}.sha256", f"{path}.sha256", client=client)

  print(f"sha256 : {sha}")
  print(f"👉  Import (pod geo-import) : geo-import stage {name}")
  print(f"    Import local : just geo-import {path}")


if __name__ == "__main__":
  app()
