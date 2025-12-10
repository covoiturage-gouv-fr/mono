import os
import logging
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

def export_query_to_file(conn, query: str, output_path: str, format: str = "parquet", chunksize: int = 100_000) -> dict:
    """
    Exporte une requête SQL vers CSV ou Parquet en streaming.
    ✅ Résout le problème de cur.description=None grâce à un fetch initial
    """
    log = logging.getLogger(__name__)
    format = format.lower()
    if format not in ("csv", "parquet"):
        raise ValueError("Format supporté : csv | parquet")

    dirname = os.path.dirname(output_path)
    if dirname:
        os.makedirs(dirname, exist_ok=True)

    total_rows = 0
    columns_count = 0

    log.info(f"▶️ Export SQL → {output_path} ({format})")
    log.info(f"🔍 Query preview: {query[:200]}...")

    try:
        with conn.cursor(name="export_cursor") as cur:
            cur.itersize = chunksize
            log.info("📊 Executing query...")
            cur.execute(query)
            log.info("✅ Query executed, fetching first chunk...")

            # ----------------------------
            # FETCH INITIAL pour remplir description
            # ----------------------------
            rows = cur.fetchmany(chunksize)
            if not rows:
                raise RuntimeError("La requête ne retourne aucune ligne")
            
            column_names = [desc[0] for desc in cur.description]

            # =========================
            # MODE CSV
            # =========================
            if format == "csv":
                first_chunk = True
                df = pd.DataFrame(rows, columns=column_names)
                df = df.convert_dtypes()
                df.to_csv(output_path, mode="w", index=False, header=True)
                total_rows += len(df)
                columns_count = len(df.columns)
                first_chunk = False
                log.info(f"💾 CSV → {total_rows} rows so far")

                # poursuivre avec les chunks suivants
                for rows in iter(lambda: cur.fetchmany(chunksize), []):
                    if not rows:
                        break
                    df = pd.DataFrame(rows, columns=column_names)
                    df = df.convert_dtypes()
                    df.to_csv(output_path, mode="a", index=False, header=False)
                    total_rows += len(df)
                    log.info(f"💾 CSV → {total_rows} rows so far")

            # =========================
            # MODE PARQUET
            # =========================
            else:
                writer = None
                chunk_num = 0

                # premier chunk
                df = pd.DataFrame(rows, columns=column_names)
                df = df.convert_dtypes()
                table = pa.Table.from_pandas(df, preserve_index=False)
                writer = pq.ParquetWriter(output_path, table.schema, compression="zstd")
                writer.write_table(table)
                chunk_num += 1
                total_rows += len(df)
                columns_count = len(df.columns)
                log.info(f"💾 Parquet chunk {chunk_num} → {total_rows} rows so far")

                # chunks suivants
                for rows in iter(lambda: cur.fetchmany(chunksize), []):
                    if not rows:
                        break
                    df = pd.DataFrame(rows, columns=column_names)
                    df = df.convert_dtypes()
                    table = pa.Table.from_pandas(df, schema=writer.schema, preserve_index=False)
                    writer.write_table(table)
                    chunk_num += 1
                    total_rows += len(df)
                    log.info(f"💾 Parquet chunk {chunk_num} → {total_rows} rows so far")

                if writer:
                    log.info("🔒 Closing ParquetWriter...")
                    writer.close()
                    log.info("✅ ParquetWriter closed")

        # Vérification finale
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
