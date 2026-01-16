import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from utils.loading import load_parquet_chunked

# dictionnaire global des colonnes et types
COLUMNS_TYPES = {
  "_id": "BIGINT",
  "created_at": "TIMESTAMP", 
  "updated_at": "TIMESTAMP",
  "operator_id": "BIGINT",
  "operator_name": "VARCHAR",
  "operator_siret": "VARCHAR",
  "operator_journey_id": "VARCHAR",
  "operator_trip_id": "VARCHAR",
  "operator_class": "VARCHAR",
  "start_datetime": "TIMESTAMP",
  "start_datetime_tz": "TIMESTAMP",
  "start_position_x": "FLOAT4",
  "start_position_y": "FLOAT4",
  "start_h3_index": "VARCHAR",
  "start_geo_code": "VARCHAR",
  "end_datetime": "TIMESTAMP",
  "end_datetime_tz": "TIMESTAMP",
  "end_position_x": "FLOAT4",
  "end_position_y": "FLOAT4",
  "end_h3_index": "VARCHAR",
  "end_geo_code": "VARCHAR",
  "geo_errors": "TEXT",
  "geo_updated_at": "TIMESTAMP",
  "distance": "INTEGER",
  "duration": "BIGINT",
  "licence_plate": "VARCHAR",
  "driver_identity_key": "VARCHAR",
  "driver_operator_user_id": "VARCHAR",
  "driver_phone": "VARCHAR",
  "driver_phone_trunc": "VARCHAR",
  "driver_id": "VARCHAR",
  "driver_travelpass_name": "VARCHAR",
  "driver_travelpass_user_id": "VARCHAR",
  "driver_revenue": "INTEGER",
  "passenger_identity_key": "VARCHAR",
  "passenger_operator_user_id": "VARCHAR",
  "passenger_phone": "VARCHAR",
  "passenger_phone_trunc": "VARCHAR",
  "passenger_id": "VARCHAR",
  "passenger_travelpass_name": "VARCHAR",
  "passenger_travelpass_user_id": "VARCHAR",
  "passenger_over_18": "BOOLEAN",
  "passenger_seats": "INTEGER",
  "passenger_contribution": "INTEGER",
  "passenger_payments": "JSONB",
  "operator_incentives_sirets": "VARCHAR[]",
  "operator_incentives_amount_total": "INTEGER",
  "policy_id": "VARCHAR",
  "policy_incentives_amount_total": "INTEGER",
  "policy_incentives_result_total": "INTEGER",
  "fraud_status": "VARCHAR",
  "fraud_labels": "VARCHAR[]",
  "anomaly_status": "VARCHAR",
  "anomaly_labels": "VARCHAR[]",
  "acquisition_status": "VARCHAR",
  "status_updated_at": "TIMESTAMP",
  "final_acquisition_status": "VARCHAR",
  "valid_acquisition_status": "BOOLEAN",
  "uuid": "VARCHAR",
  "legacy_id": "BIGINT",
}

@model(
    "raw_zone.journeys_2020",
    kind="FULL",
    columns=COLUMNS_TYPES,
    grain="_id",
    tags=["raw","journeys","2020"],
)
def execute(
    context: ExecutionContext,
    start: datetime,
    end: datetime,
    execution_time: datetime,
    **kwargs: t.Any,
) -> t.Generator[pd.DataFrame, None, None]:
    
    total_rows = 0
    chunk_count = 0
    for chunk_df in load_parquet_chunked(
        path_or_bucket="geo-datasets-archives",
        key="exports/journeys_2020.parquet",
        column_types=COLUMNS_TYPES,
        chunk_size=100_000,
    ):
        chunk_count += 1
        total_rows += len(chunk_df)
        print(f"  📦 Chunk {chunk_count}: {len(chunk_df):,} lignes | Total: {total_rows:,}")
        yield chunk_df
    print(f"✅ Import terminé: {total_rows:,} lignes en {chunk_count} chunks")
