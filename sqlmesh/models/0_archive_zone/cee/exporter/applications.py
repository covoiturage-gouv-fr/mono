import pandas as pd
from sqlmesh import ExecutionContext, model
from utils.upload import upload_to_s3, s3_file_exists

MODEL_COLUMNS = {
    "status":       "VARCHAR",
    "bucket":       "VARCHAR",
    "key":          "VARCHAR",
    "format":       "VARCHAR",
    "size_bytes":   "BIGINT",
    "rows":         "BIGINT",
    "columns":      "INTEGER",
    "date_uploaded": "TIMESTAMP",
}

COLUMNS_TYPES = [
    ("uuid",                  "UUID",      "uuid"),
    ("carpool_v2_id",         "BIGINT",    "carpool_v2_id"),
    ("operator_id",           "BIGINT",    "operator_id"),
    ("operator_journey_id",   "VARCHAR",   "operator_journey_id"),
    ("datetime",              "TIMESTAMP", "datetime"),
    ("journey_type",          "VARCHAR",   "journey_type"),
    ("is_specific",           "BOOLEAN",   "is_specific"),
    ("application_timestamp", "TIMESTAMP", "application_timestamp"),
    ("created_at",            "TIMESTAMP", "created_at"),
    ("updated_at",            "TIMESTAMP", "updated_at"),
]

@model(
    "archive_zone.archive_cee_applications",
    columns=MODEL_COLUMNS,
    tags=["archive", "cee"],
    depends_on = ["archive_zone.cee_applications"]
)
def execute(context: ExecutionContext, **kwargs):
    from utils.export_data import build_select_query, export_query_to_file

    s3_key = f"exports/cee_applications.parquet"

    if s3_file_exists(s3_key):
        print(f"--- Skipping: s3://{s3_key} already exists ---")
        yield from ()
        return


    query = build_select_query(COLUMNS_TYPES, "archive_zone.cee_applications")
    output_file = "/tmp/cee_applications.parquet"

    print("--- Exporting CEE applications ---")
    export_info = export_query_to_file(
        conn = context.engine_adapter.connection,
        query = query,
        columns = COLUMNS_TYPES,
        output_path = output_file,
        format = "parquet",
        chunksize = 100000,
    )

    upload_info = upload_to_s3(file_path=output_file, key=s3_key)

    yield pd.DataFrame([{
        "status":       "uploaded",
        "bucket":       upload_info["bucket"],
        "key":          upload_info["key"],
        "format":       upload_info["format"],
        "size_bytes":   upload_info["size_bytes"],
        "rows":         export_info["rows"],
        "columns":      export_info["columns"],
        "date_uploaded": upload_info["date_uploaded"],
    }])
