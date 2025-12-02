import io
import typing as t
import pandas as pd
from sqlmesh import ExecutionContext, model
from utils.upload import upload_to_s3

@model(
    "archive_zone.archive_journeys_2024",
    kind="FULL",
    columns={
      "status": "VARCHAR",
      "bucket": "VARCHAR",
      "key": "VARCHAR",
      "format": "VARCHAR",
      "size_bytes": "INTEGER",
      "rows": "INTEGER",
      "columns": "INTEGER",
      "date_uploaded": "TIMESTAMP",
    },
    tags=["archive","journeys_2024"],
)
def execute(
    context: ExecutionContext,
    **kwargs: t.Any,
) -> pd.DataFrame:
    df = context.fetchdf("SELECT * FROM trusted_zone.journeys_2024")
    upload_result = upload_to_s3(
      key="exports/journeys_2024.parquet",
      df=df,
  )

    return pd.DataFrame([upload_result])
