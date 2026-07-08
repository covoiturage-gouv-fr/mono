import os
import subprocess
import time
from typing import Optional
from pipelines.helpers.duckdb import duckdb_client
from pipelines.helpers.sql import create_schema, build_select

GEO_EXTS = ("gpkg", "geojson", "shp")


def import_table(
  table: str,
  schema: str,
  path: str,
  ext: str = "parquet",
  geo_layer: Optional[str] = None,
  select: Optional[list[str | list[str]]] = None,
  conn=None,
) -> int:
  _conn = conn or duckdb_client()
  create_schema(_conn, schema)

  print(f"▶️  Import {path} → {schema}.{table}")
  t0 = time.monotonic()
  try:
    if ext in GEO_EXTS:
      rows = _import_geo(_conn, table, schema, path, geo_layer, select)
    else:
      rows = _import_tabular(_conn, table, schema, path, ext, select)
  except Exception:
    # postgres_execute : le drop passe même si ogr2ogr a créé la table hors du cache catalogue DuckDB.
    _conn.execute(f"CALL postgres_execute('pg', 'DROP TABLE IF EXISTS {schema}.{table}')")  # pas de table à moitié remplie
    raise
  elapsed = time.monotonic() - t0

  rows_fmt = f"{rows:_}".replace("_", " ")  # séparateur de milliers à la française
  print(f"✅ {schema}.{table} — {rows_fmt} lignes en {elapsed:.1f}s")

  if not conn:
    _conn.close()

  return rows


def _import_tabular(conn, table, schema, path, ext, select) -> int:
  select_clause = build_select(select)
  if ext == "csv":
    source_sql = f"read_csv_auto('{path}')"
  elif ext in ("xlsx", "xls"):
    source_sql = f"read_excel('{path}')"
  elif ext == "parquet":
    source_sql = f"read_parquet('{path}')"
  else:
    raise ValueError(f"Extension non supportée : {ext}")
  # Une passe : CREATE TABLE AS streame vers Postgres et renvoie le nombre de lignes.
  return conn.execute(f"CREATE TABLE pg.{schema}.{table} AS SELECT {select_clause} FROM {source_sql};").fetchone()[0]


def _import_geo(conn, table, schema, path, geo_layer, select) -> int:
  """Charge une couche géo via ogr2ogr (streaming natif → PostGIS), plus stable que duckdb-spatial."""
  cmd = [
    "ogr2ogr", "-f", "PostgreSQL", "PG:", path,
    "-nln", f"{schema}.{table}",
    "-lco", f"GEOMETRY_NAME={_geom_name(select)}",
    "-lco", "FID=ogc_fid",  # normalise le nom de la PK quel que soit le FID source (ex. couche « simple » : id)
    "-nlt", "PROMOTE_TO_MULTI",  # les couches IGN mêlent Polygon/MultiPolygon
    "-overwrite", "--config", "PG_USE_COPY", "YES",
  ]
  ogr_sql = _build_ogr_sql(select, geo_layer)
  if ogr_sql:
    # OGRSQL : mêmes noms de champs que duckdb st_read (le dialecte SQLite natif du GPKG diffère).
    cmd += ["-dialect", "OGRSQL", "-sql", ogr_sql]
  elif geo_layer:
    cmd.append(geo_layer)  # couche entière, sans projection

  # Mot de passe passé par l'environnement libpq (jamais dans argv/ps).
  env = {
    **os.environ,
    "PGHOST": os.getenv("DBT_HOST", ""), "PGPORT": os.getenv("DBT_PORT", ""),
    "PGUSER": os.getenv("DBT_USER", ""), "PGPASSWORD": os.getenv("DBT_PASSWORD", ""),
    "PGDATABASE": os.getenv("DBT_DBNAME", ""),
  }
  proc = subprocess.run(cmd, env=env, capture_output=True, text=True)
  if proc.returncode != 0:
    raise RuntimeError(f"ogr2ogr a échoué ({proc.returncode}) : {proc.stderr.strip()[:500]}")

  # ogc_fid (PK serial ajoutée par ogr2ogr) est conservée : idiomatique, invisible en aval
  # (les modèles lisent les colonnes par nom). postgres_query contourne le cache catalogue DuckDB.
  return conn.execute(
    f"SELECT n FROM postgres_query('pg', 'SELECT count(*)::bigint AS n FROM {schema}.{table}')"
  ).fetchone()[0]


def _geom_name(select) -> str:
  """Nom de la colonne géométrie en sortie (alias du champ geometry de la config, défaut « geom »)."""
  for item in select or []:
    if isinstance(item, list) and len(item) == 3 and item[1].lower() == "geometry":
      return item[2].strip() or "geom"
  return "geom"


def _build_ogr_sql(select, geo_layer) -> Optional[str]:
  """Traduit le `select` de la config en OGR SQL. None si pas de select (couche entière copiée telle quelle)."""
  if not select:
    return None
  cols = []
  for item in select:
    if isinstance(item, list):
      col, dtype, alias = item
      # La géométrie source est reprise telle quelle (renommée en sortie via -lco GEOMETRY_NAME).
      cols.append(col if dtype.lower() == "geometry" else f"{col} AS {alias.strip()}")
    else:
      cols.append(item)
  return f'SELECT {", ".join(cols)} FROM "{geo_layer}"'


def export_table(
    table: str,
    schema: str,
    path: str,
    ext: str = "parquet",
    select: Optional[list[str | list[str]]] = None,
    where: Optional[str] = None,
    partition_by: Optional[list[str]] = None,
    conn=None,
):
    _conn = conn or duckdb_client()

    print(f"▶️ Export {schema}.{table} → {path}")
    select_clause = build_select(select)
    where_clause = f"WHERE {where}" if where else ""
    partition_clause = ""
    if partition_by:
        cols = ", ".join(partition_by)
        partition_clause = f", PARTITION_BY ({cols})"
    if ext =="parquet":
      format_clause = f"(FORMAT {ext.upper()}{partition_clause})"
    elif ext == "csv":
      format_clause = f"(FORMAT {ext.upper()}, DELIMITER ',', HEADER)"
    elif ext == "json":
      format_clause = f"(FORMAT {ext.upper()})"
    else:
      raise ValueError(f"Extension non supportée : {ext}")
    sql = f"""
    COPY (
        SELECT {select_clause} FROM pg.{schema}.{table} {where_clause}
    )
    TO '{path}'
    {format_clause};
    """

    _conn.execute(sql)
    print(f"✅ Export terminé : {path}")

    if not conn:
        _conn.close()
