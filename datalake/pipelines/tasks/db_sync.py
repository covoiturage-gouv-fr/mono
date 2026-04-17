from typing import Optional
from pipelines.helpers.duckdb import duckdb_client
from pipelines.helpers.sql import create_schema, build_select


def import_table(
  table: str,
  schema: str,
  path: str,
  ext: str = "parquet",
  geo_layer: Optional[str] = None,
  select: Optional[list[str | list[str]]] = None,
  conn=None,
):
  _conn = conn or duckdb_client()
  create_schema(_conn, schema)

  print(f"▶️ Import {path} → {schema}.{table}")
  select_clause = build_select(select)
  sql = f"CREATE TABLE pg.{schema}.{table} AS SELECT {select_clause} FROM "
  if ext in ("gpkg", "geojson", "shp"):
    layer_clause = f", layer='{geo_layer}'" if geo_layer else ""
    sql += f"st_read('{path}'{layer_clause});"
  elif ext == "csv":
    sql += f"read_csv_auto('{path}');"
  elif ext in ("xlsx", "xls"):
    sql += f"read_excel('{path}');"
  elif ext == "parquet":
    sql += f"read_parquet('{path}');"
  else:
    raise ValueError(f"Extension non supportée : {ext}")

  _conn.execute(sql)
  print(f"✅ Import terminé : {schema}.{table}")

  if not conn:
      _conn.close()


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
