import pandas as pd
from datetime import datetime
from zoneinfo import ZoneInfo
from sqlmesh import ExecutionContext, model
from utils.upload import upload_to_s3

COLUMNS_TYPES = [
  ("_id", "BIGINT", "_id"),
  ("carpool_v2_id", "BIGINT", "carpool_v2_id"),
  ("datetime", "TIMESTAMP", "datetime"),
  ("operator_id", "BIGINT", "operator_id"),
  ("operator_journey_id", "VARCHAR", "operator_journey_id"),
  ("campaign_id", "BIGINT", "campaign_id"),
  ("campaign_name", "VARCHAR", "campaign_name"),
  ("territory_siret", "VARCHAR", "territory_siret"),
  ("territory_name", "VARCHAR", "territory_name"),
  ("amount", "INTEGER", "amount"),
  ("result", "INTEGER", "result"),
  ("status", "VARCHAR", "status"),
  ("state", "VARCHAR", "state"),
]

@model(
    "archive_zone.archive_incentives",
    kind="FULL",
    columns={
        "start": "VARCHAR",
        "end": "VARCHAR",
        "status": "VARCHAR",
        "bucket": "VARCHAR",
        "key": "VARCHAR",
        "format": "VARCHAR",
        "size_bytes": "BIGINT",
        "rows": "BIGINT",
        "columns": "INTEGER",
        "date_uploaded": "TIMESTAMP",
    },
    tags=["archive", "incentives"],
)
def execute(context: ExecutionContext, **kwargs):
    from utils.export_data import build_select_query, export_query_to_file

    tz = ZoneInfo("Europe/Paris")
    file_format = "parquet"
    chunksize = 100_000
    conn = context.engine_adapter.connection

    first_year = 2019
    last_year = datetime.now(tz).year - 1  # last completed year

    results = []
    for year in range(first_year, last_year + 1):
        start = datetime(year, 1, 1, tzinfo=tz)
        end = datetime(year + 1, 1, 1, tzinfo=tz)

        query = (
            build_select_query(COLUMNS_TYPES, "archive_zone.archive_incentives_view")
            + f" WHERE datetime >= '{start.isoformat()}' AND datetime < '{end.isoformat()}'"
        )
        output_file = f"/tmp/incentives_{year}.parquet"

        print(f"--- Exporting {year} ---")
        export_info = export_query_to_file(
            conn=conn,
            query=query,
            columns=COLUMNS_TYPES,
            output_path=output_file,
            format=file_format,
            chunksize=chunksize,
        )

        upload_info = upload_to_s3(
            file_path=output_file,
            key=f"exports/incentives_{year}.{file_format.lower()}",
        )

        results.append(
            {
                "start": start.isoformat(),
                "end": end.isoformat(),
                "status": "uploaded",
                "bucket": upload_info["bucket"],
                "key": upload_info["key"],
                "format": upload_info["format"],
                "size_bytes": upload_info["size_bytes"],
                "rows": export_info["rows"],
                "columns": export_info["columns"],
                "date_uploaded": upload_info["date_uploaded"],
            }
        )

    return pd.DataFrame(results)
