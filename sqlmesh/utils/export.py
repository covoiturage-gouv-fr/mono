import os
import logging
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

def export_query_to_file(
    conn,               # DBAPI connection (psycopg2)
    query: str,
    output_path: str,
    format: str = "parquet",
    chunksize: int = 100_000,
) -> dict:
    """
    Exporte une requête SQL vers CSV ou Parquet en streaming à partir d'une connexion DBAPI.
    """
    log = logging.getLogger(__name__)
    format = format.lower()
    if format not in ("csv", "parquet"):
        raise ValueError("Format supporté : csv | parquet")

    os.makedirs(os.path.dirname(output_path), exist_ok=True)
    total_rows = 0
    columns_count = 0

    log.info(f"▶️ Export SQL → {output_path} ({format})")

    # CSV
    if format == "csv":
        first_chunk = True
        with conn.cursor(name="export_cursor") as cur:  # server-side cursor
            cur.itersize = chunksize
            cur.execute(query)

            for rows in iter(lambda: cur.fetchmany(chunksize), []):
                df = pd.DataFrame(rows, columns=[desc[0] for desc in cur.description])
                columns_count = len(df.columns)
                total_rows += len(df)
                df.to_csv(
                    output_path,
                    mode="w" if first_chunk else "a",
                    index=False,
                    header=first_chunk,
                )
                first_chunk = False

    # Parquet
    else:
        writer = None
        with conn.cursor(name="export_cursor") as cur:
            cur.itersize = chunksize
            cur.execute(query)

            for rows in iter(lambda: cur.fetchmany(chunksize), []):
                df = pd.DataFrame(rows, columns=[desc[0] for desc in cur.description])
                table = pa.Table.from_pandas(df, preserve_index=False)
                columns_count = len(df.columns)
                total_rows += len(df)
                if writer is None:
                    writer = pq.ParquetWriter(output_path, table.schema, compression="zstd")
                writer.write_table(table)

        if writer:
            writer.close()

    file_size = os.path.getsize(output_path)
    log.info(f"✅ Export terminé : {total_rows} lignes | {file_size / 1024 / 1024:.1f} MB")

    return {
        "rows": total_rows,
        "columns": columns_count,
        "size_bytes": file_size,
        "path": output_path,
        "format": format,
    }
