import os
import logging
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

def export_query_to_file(
    conn,
    query: str,
    output_path: str,
    format: str = "parquet",
    chunksize: int = 100_000,
) -> dict:
    """
    Exporte une requête SQL vers CSV ou Parquet.
    """
    log = logging.getLogger(__name__)
    format = format.lower()

    if format not in ("csv", "parquet"):
        raise ValueError("Format supporté : csv | parquet")

    # Création du dossier si nécessaire
    dirname = os.path.dirname(output_path)
    if dirname:
        os.makedirs(dirname, exist_ok=True)

    total_rows = 0
    columns_count = 0

    try:
        with conn.cursor(name="export_cursor") as cur:
            cur.itersize = chunksize
            log.info("📊 Executing query...")
            cur.execute(query)
            log.info("✅ Query executed, fetching data...")

            column_names = [desc[0] for desc in cur.description]

            if format == "csv":
                first_chunk = True
                for rows in iter(lambda: cur.fetchmany(chunksize), []):
                    if not rows:
                        break

                    df = pd.DataFrame(rows, columns=column_names)
                    df = df.convert_dtypes()  # ✅ stabilise tous les types nullable

                    df.to_csv(
                        output_path,
                        mode="w" if first_chunk else "a",
                        index=False,
                        header=first_chunk
                    )

                    total_rows += len(df)
                    columns_count = len(df.columns)
                    first_chunk = False
                    log.info(f"💾 CSV → {total_rows} rows")

            else:  # parquet
                writer = None
                chunk_num = 0

                for rows in iter(lambda: cur.fetchmany(chunksize), []):
                    if not rows:
                        break

                    chunk_num += 1
                    df = pd.DataFrame(rows, columns=column_names)
                    df = df.convert_dtypes()  # ✅ stabilise les types
                    total_rows += len(df)
                    columns_count = len(df.columns)

                    # Créer / appliquer le schema Parquet
                    if writer is None:
                        table = pa.Table.from_pandas(df, preserve_index=False)
                        writer = pq.ParquetWriter(output_path, table.schema, compression="zstd")
                        log.info(f"📝 ParquetWriter created at {output_path}")
                    else:
                        table = pa.Table.from_pandas(df, schema=writer.schema, preserve_index=False)

                    writer.write_table(table)
                    log.info(f"💾 Parquet chunk {chunk_num} → {total_rows} rows")

                if writer:
                    log.info("🔒 Closing ParquetWriter...")
                    writer.close()
                    log.info("✅ ParquetWriter closed")

        # Vérification du fichier final
        if not os.path.exists(output_path):
            raise FileNotFoundError(f"Le fichier n'a pas été créé: {output_path}")

        file_size = os.path.getsize(output_path)
        log.info(f"✅ Export terminé : {total_rows} lignes | {file_size / 1024 / 1024:.1f} MB")

        return {
            "rows": total_rows,
            "columns": columns_count,
            "size_bytes": file_size,
            "path": output_path,
            "format": format,
        }

    except Exception as e:
        log.error(f"❌ Erreur pendant l'export: {str(e)}", exc_info=True)
        raise
