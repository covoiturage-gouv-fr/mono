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
    Exporte une requête SQL vers CSV ou Parquet en streaming.
    """
    log = logging.getLogger(__name__)
    format = format.lower()
    
    if format not in ("csv", "parquet"):
        raise ValueError("Format supporté : csv | parquet")
    
    # Vérifier si dirname n'est pas vide avant de créer
    dirname = os.path.dirname(output_path)
    if dirname:
        os.makedirs(dirname, exist_ok=True)
    
    log.info(f"▶️ Export SQL → {output_path} ({format})")
    log.info(f"🔍 Query: {query[:200]}...")  # Log les 200 premiers caractères
    
    total_rows = 0
    columns_count = 0
    
    try:
        # CSV
        if format == "csv":
            first_chunk = True
            with conn.cursor(name="export_cursor") as cur:
                log.info("📊 Executing query...")
                cur.itersize = chunksize
                cur.execute(query)
                log.info("✅ Query executed, fetching data...")
                
                for rows in iter(lambda: cur.fetchmany(chunksize), []):
                    if not rows:
                        break
                    
                    log.info(f"📦 Fetched {len(rows)} rows")
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
                    log.info(f"💾 Written {total_rows} total rows so far")
        
        # Parquet
        else:
            writer = None
            with conn.cursor(name="export_cursor") as cur:
                log.info("📊 Executing query...")
                cur.itersize = chunksize
                cur.execute(query)
                log.info("✅ Query executed, fetching data...")
                
                chunk_num = 0
                for rows in iter(lambda: cur.fetchmany(chunksize), []):
                    if not rows:
                        break
                    
                    chunk_num += 1
                    log.info(f"📦 Chunk {chunk_num}: Fetched {len(rows)} rows")
                    
                    df = pd.DataFrame(rows, columns=[desc[0] for desc in cur.description])
                    log.info(f"🔄 DataFrame created with {len(df)} rows, {len(df.columns)} columns")
                    
                    table = pa.Table.from_pandas(df, preserve_index=False)
                    log.info(f"🔄 Arrow table created")
                    
                    columns_count = len(df.columns)
                    total_rows += len(df)
                    
                    if writer is None:
                        log.info(f"📝 Creating ParquetWriter at {output_path}")
                        writer = pq.ParquetWriter(output_path, table.schema, compression="zstd")
                        log.info(f"✅ ParquetWriter created")
                    
                    writer.write_table(table)
                    log.info(f"💾 Written chunk {chunk_num} ({total_rows} total rows so far)")
            
            if writer:
                log.info("🔒 Closing ParquetWriter...")
                writer.close()
                log.info("✅ ParquetWriter closed")
        
        # Vérifier que le fichier existe
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
