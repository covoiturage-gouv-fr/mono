import typer
from pipelines.tasks.db_sync import export_table
from pipelines.helpers.duckdb import duckdb_client
from pipelines.helpers.sql import get_existing_tables
from pipelines.helpers.s3 import s3_client, s3_exists, s3_path
from pipelines.helpers.config import load_config
from typing import Optional
from dotenv import load_dotenv

load_dotenv()
app = typer.Typer()


@app.command()
def export(
    config: str,
    schema: str,
    bucket: Optional[str] = typer.Option(default=None, envvar="S3_BUCKET"),
    folder: Optional[str] = None,
    overwrite: bool = False,
):
    tables = load_config(config)
    conn = duckdb_client()
    s3 = s3_client()
    existing_tables = get_existing_tables(conn, schema)
    for t in tables:
        name = t["name"]
        filename = t.get("filename", name)
        ext = t.get("ext", "parquet")
        select = t.get("select")
        where = t.get("where")
        partition_by = t.get("partition_by")
        key, path = s3_path(filename, ext, bucket, folder)
        if name not in existing_tables:
            print(f"❌ [{name}] Table absente dans {schema}, skipping.")
            continue
        if s3_exists(bucket, key, s3) and not overwrite:
            print(f"⏭️  [{filename}] Déjà présent sur S3, skipping.")
            continue
        export_table(table=name, schema=schema, path=path, ext=ext, select=select, where=where, partition_by=partition_by, conn=conn)
    conn.close()


if __name__ == "__main__":
    app()