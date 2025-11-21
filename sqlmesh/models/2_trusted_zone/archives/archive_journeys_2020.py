import io
import typing as t
import pandas as pd
from sqlmesh import ExecutionContext, model
from utils.s3 import get_s3_client

@model(
    "trusted_zone.archive_journeys_2020",
    kind="FULL",
    columns={
      "status": "VARCHAR",
    },
    tags=["trusted","archive","journeys_2020"],
)
def execute(
    context: ExecutionContext,
    **kwargs: t.Any,
) -> pd.DataFrame:
    df = context.fetchdf("SELECT * FROM trusted_zone.journeys_2020")
    print(f"✅ Données chargées : {len(df)} lignes")
    # 2️⃣ Initialiser S3
    s3_client = get_s3_client()
    bucket_name = "geo-data-archives"
    
    # 3️⃣ Exporter en Parquet
    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False)
    buffer.seek(0)
    
    s3_client.put_object(
        Bucket=bucket_name,
        Key="exports/journeys_2020.parquet",
        Body=buffer.getvalue(),
    )
    print(f"✅ Parquet uploadé : s3://{bucket_name}/exports/journeys_2020.parquet")

    return pd.DataFrame({"status": ["exported"]})
