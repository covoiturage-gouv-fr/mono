from config.raw import RAW_TABLES
from helpers.duckdb import get_existing_tables
from helpers.s3 import s3_exists, s3_path
from typing import Optional

def import_raw(
  tables: list[dict[str, str]] = None,
  schema: Optional[str] = None,
  bucket: Optional[str] = None,
  folder: Optional[str] = None,
  overwrite: bool = False,):
  tables = tables or RAW_TABLES 
  schema = schema or 'dbt_raw'
  existing_tables = get_existing_tables(schema)
  for table, ext, _ in tables:
    if not s3_exists(table, ext):
        raise RuntimeError(f"❌ Missing: {s3_path(table, ext)}")

        print(f"✅ {s3.key(table, ext)}")
  for table, ext, layer in tables:
    if table in existing_tables:
      print(f"⚠️ Table {schema}.{table} existe déjà, skipping import.")
      continue
    import_table(table=table, schema=schema, bucket=bucket, folder=folder, ext=ext,  geo_layer=layer, overwrite=overwrite)
