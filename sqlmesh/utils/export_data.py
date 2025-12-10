import os
import logging
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

def build_select_query(columns, table):
  select_list = [
    f"{expr}::{pg_type} AS {alias}"
    for expr, pg_type, alias in columns
  ]
  return f"SELECT {', '.join(select_list)} FROM {table}"

PG_TO_ARROW = {
  "BIGINT": pa.int64(),
  "INTEGER": pa.int32(),
  "BOOLEAN": pa.bool_(),
  "VARCHAR": pa.string(),
  "TEXT": pa.string(),
  "TIMESTAMP": pa.timestamp("us"),
  "DATE": pa.date32(),
  "TIME": pa.time64("us"),
  "BYTEA": pa.binary(),
  "FLOAT": pa.float64(),
  "FLOAT4": pa.float32(),
  "FLOAT8": pa.float64(),   
  "REAL": pa.float32(),
  "DOUBLE PRECISION": pa.float64(),
  "UUID": pa.string(),
  "JSON": pa.string(),
  "JSONB": pa.string(),
  "BIGINT[]": pa.list_(pa.int64()),
  "INTEGER[]": pa.list_(pa.int32()),
  "BOOLEAN[]": pa.list_(pa.bool_()),
  "VARCHAR[]": pa.list_(pa.string()),
  "TEXT[]": pa.list_(pa.string())
}

def build_schema(columns):
  fields = []
  for _, pg_type, alias in columns:
    if pg_type not in PG_TO_ARROW:
      raise ValueError(f"Type Postgres non supporte: {pg_type}")
    arrow_type = PG_TO_ARROW[pg_type]
    fields.append(pa.field(alias, arrow_type, nullable=True))
  return pa.schema(fields)

def export_query_to_file(conn, query: str, columns: list, output_path: str, format: str = "parquet", chunksize: int = 100_000) -> dict:
    """
    Exporte une requete SQL vers CSV ou Parquet en streaming.
    Resout le probleme de cur.description=None grace a un fetch initial
    """
    log = logging.getLogger(__name__)
    format = format.lower()
    if format not in ("csv", "parquet"):
        raise ValueError("Format supporte : csv | parquet")

    dirname = os.path.dirname(output_path)
    if dirname:
        os.makedirs(dirname, exist_ok=True)

    total_rows = 0
    columns_count = len(columns)
    schema = build_schema(columns)
    column_names = [alias for _, _, alias in columns]

    try:
      with conn.cursor(name="export_cursor") as cur:
          cur.itersize = chunksize
          log.info("Executing query...")
          cur.execute(query)
          log.info("Query executed, fetching first chunk...")

          if format == "csv":
            first_chunk = True
            for rows in iter(lambda: cur.fetchmany(chunksize), []):
              if not rows:
                break
              df = pd.DataFrame(rows, columns=column_names)
              df.to_csv(output_path, mode="w" if first_chunk else "a",
                        index=False, header=first_chunk)
              first_chunk = False
              total_rows += len(df)
              log.info(f"CSV -> {total_rows} rows")
          else:
              writer = pq.ParquetWriter(output_path, schema, compression="zstd")
              chunk_num = 0
              for rows in iter(lambda: cur.fetchmany(chunksize), []):
                if not rows:
                    break
                df = pd.DataFrame(rows, columns=column_names)
                table = pa.Table.from_pandas(df, schema=schema, preserve_index=False)
                writer.write_table(table)
                chunk_num += 1
                total_rows += len(df)
                log.info(f"Parquet chunk {chunk_num} -> {total_rows} rows")
              writer.close()
              log.info("ParquetWriter closed")
              
      if not os.path.exists(output_path):
        raise FileNotFoundError(f"Le fichier n'a pas ete cree: {output_path}")
      file_size = os.path.getsize(output_path)
      log.info(f"Export termine : {total_rows} lignes | {file_size / 1024 / 1024:.1f} MB")
      return {
        "rows": total_rows,
        "columns": columns_count,
        "size_bytes": file_size,
        "path": output_path,
        "format": format,
      }
    except Exception as e:
      log.error(f"Erreur pendant l'export: {str(e)}", exc_info=True)
      raise