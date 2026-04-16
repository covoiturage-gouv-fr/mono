from typing import Optional
from pipelines.helpers.duckdb import duckdb_client, create_schema

def import_table(
    table: str,
    schema: str,
    path: str,
    ext: str = "parquet",
    geo_layer: Optional[str] = None,
    conn=None,
    select: Optional[list[str] | list[tuple[str, str]]] = None,
):
    """Importe un fichier S3 dans Postgres. Aucune vérification ici — à faire en amont."""
    _conn = conn or duckdb_client()
    create_schema(_conn, schema)

    print(f"▶️ Import {path} → {schema}.{table}")
    select_clause = build_select(select)
    if ext in ("gpkg", "geojson", "shp"):
        layer_clause = f", layer='{geo_layer}'" if geo_layer else ""
        sql = f"CREATE TABLE pg.{schema}.{table} AS SELECT {select_clause} FROM st_read('{path}'{layer_clause});"
    elif ext == "csv":
        sql = f"CREATE TABLE pg.{schema}.{table} AS SELECT {select_clause} FROM read_csv_auto('{path}');"
    elif ext in ("xlsx", "xls"):
        sql = f"CREATE TABLE pg.{schema}.{table} AS SELECT {select_clause} FROM read_excel('{path}');"
    elif ext == "parquet":
        sql = f"CREATE TABLE pg.{schema}.{table} AS SELECT {select_clause} FROM read_parquet('{path}');"
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
    conn=None,
    select: Optional[list[str] | list[tuple[str, str]]] = None,
    partition_by: Optional[list[str]] = None,
):
    """Exporte une table Postgres vers S3. Aucune vérification ici — à faire en amont."""
    _conn = conn or duckdb_client()

    print(f"▶️ Export {schema}.{table} → {path}")
    select_clause = build_select(select)
    partition_clause = ""
    if partition_by:
        cols = ", ".join(partition_by)
        partition_clause = f", PARTITION_BY ({cols})"

    sql = f"""
    COPY (
        SELECT {select_clause} FROM pg.{schema}.{table}
    )
    TO '{path}'
    (FORMAT PARQUET{partition_clause});
    """

    _conn.execute(sql)
    print(f"✅ Export terminé : {path}")

    if not conn:
        _conn.close()

def normalize_select(select):
  if not select:
    return None, {}
  cols = []
  casts = {}
  for item in select:
    if isinstance(item, list):
      col, dtype = item
      col = col.lower()
      cols.append(col)
      casts[col] = dtype.upper()
    else:
      col = item.lower()
      cols.append(col)
  return cols, casts

def build_select(select):
  cols, casts = normalize_select(select)
  if not cols:
    return "*"
  sql = []
  for col in cols:
    if col in casts:
      sql.append(f"CAST({col} AS {casts[col]}) AS {col}")
    else:
      sql.append(col)
  return ", ".join(sql)