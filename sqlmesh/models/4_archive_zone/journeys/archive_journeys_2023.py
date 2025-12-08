import pandas as pd
from sqlmesh import ExecutionContext, model
from utils.upload import upload_to_s3
from utils.export import export_query_to_file  # la fonction que nous avons adaptée

@model(
    "archive_zone.archive_journeys_2023",
    kind="FULL",
    columns={
        "status": "VARCHAR",
        "bucket": "VARCHAR",
        "key": "VARCHAR",
        "format": "VARCHAR",
        "size_bytes": "BIGINT",
        "rows": "BIGINT",
        "columns": "INTEGER",
        "date_uploaded": "TIMESTAMP",
    },
    tags=["archive","journeys_2023"],
)
def execute(context: ExecutionContext, **kwargs):

    # -----------------------------
    # Configuration
    # -----------------------------
    query = "SELECT * FROM trusted_zone.journeys_2023"
    output_file = "/tmp/journeys_2023.parquet"
    file_format = "parquet"
    chunksize = 100_000  

    # -----------------------------
    # Connexion PostgreSQL via SQLMesh
    # -----------------------------
    conn = context.engine_adapter.connection  # DBAPI psycopg2

    # -----------------------------
    # Export chunk par chunk
    # -----------------------------
    export_info = export_query_to_file(
        conn=conn,
        query=query,
        output_path=output_file,
        format=file_format,
        chunksize=chunksize,
    )

    # -----------------------------
    # Upload vers S3
    # -----------------------------
    upload_info = upload_to_s3(
        file_path=output_file,
        key=f"exports/journeys_2023.{file_format.lower()}",
    )

    # -----------------------------
    # Retour compatible SQLMesh
    # -----------------------------
    return pd.DataFrame(
        [
            {
              "status": "uploaded",
              "bucket": upload_info["bucket"],
              "key": upload_info["key"],
              "format": upload_info["format"],
              "size_bytes": upload_info["size_bytes"],
              "rows": export_info["rows"],
              "columns": export_info["columns"],
              "date_uploaded": upload_info["date_uploaded"],
            }
        ]
    )
