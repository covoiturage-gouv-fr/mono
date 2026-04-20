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
  chunk_size: Optional[int] = None,
  conn=None,
):
  _conn = conn or duckdb_client()
  create_schema(_conn, schema)

  print(f"▶️ Import {path} → {schema}.{table}")
  select_clause = build_select(select)
  if ext in ("gpkg", "geojson", "shp"):
    layer_clause = f", layer='{geo_layer}'" if geo_layer else ""
    source_sql = f"st_read('{path}'{layer_clause})"
  elif ext == "csv":
    source_sql = f"read_csv_auto('{path}')"
  elif ext in ("xlsx", "xls"):
    source_sql = f"read_excel('{path}')"
  elif ext == "parquet":
    source_sql = f"read_parquet('{path}')"
  else:
    raise ValueError(f"Extension non supportée : {ext}")
 
  if chunk_size:
    print(f"  ↳ chunks de {chunk_size}")
    offset = 0
    chunk_n = 0
    while True:
      chunk_sql = f"SELECT {select_clause} FROM {source_sql} LIMIT {chunk_size} OFFSET {offset}"
      rows = _conn.execute(chunk_sql).fetchall()
      if not rows:
        break
      if chunk_n == 0:
        _conn.execute(f"CREATE TABLE pg.{schema}.{table} AS {chunk_sql};")
      else:
        _conn.execute(f"INSERT INTO pg.{schema}.{table} {chunk_sql};")
      offset += chunk_size
      chunk_n += 1
      print(f"  ↳ chunk {chunk_n} — {offset} features insérées")
      if len(rows) < chunk_size:
        break
  else:
    _conn.execute(f"""
      CREATE TABLE pg.{schema}.{table} AS
      SELECT {select_clause} FROM {source_sql};
    """)
 
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
