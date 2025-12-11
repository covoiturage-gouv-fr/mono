import pandas as pd
from sqlmesh import ExecutionContext, model
from utils.upload import upload_to_s3



COLUMNS_TYPES = [
  ("_id", "BIGINT", "_id"),
  ("created_at", "TIMESTAMP", "created_at"),
  ("updated_at", "TIMESTAMP", "updated_at"),
  ("operator_id", "BIGINT", "operator_id"),
  ("operator_name", "VARCHAR", "operator_name"),
  ("operator_siret", "VARCHAR", "operator_siret"),
  ("operator_journey_id", "VARCHAR", "operator_journey_id"),
  ("operator_trip_id", "VARCHAR", "operator_trip_id"),
  ("operator_class", "VARCHAR", "operator_class"),
  ("start_datetime", "TIMESTAMP", "start_datetime"),
  ("start_datetime_tz", "TIMESTAMP", "start_datetime_tz"),
  ("st_x(start_position::geometry)", "FLOAT4", "start_position_x"),
  ("st_y(start_position::geometry)", "FLOAT4", "start_position_y"),
  ("start_h3_index::VARCHAR", "VARCHAR", "start_h3_index"),
  ("start_geo_code::VARCHAR", "VARCHAR", "start_geo_code"),
  ("end_datetime::TIMESTAMP", "TIMESTAMP", "end_datetime"),
  ("end_datetime_tz::TIMESTAMP", "TIMESTAMP", "end_datetime_tz"),
  ("st_x(end_position::geometry)", "FLOAT4", "end_position_x"),
  ("st_y(end_position::geometry)", "FLOAT4", "end_position_y"),
  ("end_h3_index::VARCHAR", "VARCHAR", "end_h3_index"),
  ("end_geo_code::VARCHAR", "VARCHAR", "end_geo_code"),
  ("geo_errors::TEXT", "TEXT", "geo_errors"),
  ("geo_updated_at::TIMESTAMP", "TIMESTAMP", "geo_updated_at"),
  ("distance::INTEGER", "INTEGER", "distance"),
  ("EXTRACT(EPOCH FROM duration)::BIGINT", "BIGINT", "duration"),
  ("licence_plate::VARCHAR", "VARCHAR", "licence_plate"),
  ("driver_identity_key::VARCHAR", "VARCHAR", "driver_identity_key"),
  ("driver_operator_user_id::VARCHAR", "VARCHAR", "driver_operator_user_id"),
  ("driver_phone::VARCHAR", "VARCHAR", "driver_phone"),
  ("driver_phone_trunc::VARCHAR", "VARCHAR", "driver_phone_trunc"),
  ("driver_id::VARCHAR", "VARCHAR", "driver_id"),
  ("driver_travelpass_name::VARCHAR", "VARCHAR", "driver_travelpass_name"),
  ("driver_travelpass_user_id::VARCHAR", "VARCHAR", "driver_travelpass_user_id"),
  ("driver_revenue::INTEGER", "INTEGER", "driver_revenue"),
  ("passenger_identity_key::VARCHAR", "VARCHAR", "passenger_identity_key"),
  ("passenger_operator_user_id::VARCHAR", "VARCHAR", "passenger_operator_user_id"),
  ("passenger_phone::VARCHAR", "VARCHAR", "passenger_phone"),
  ("passenger_phone_trunc::VARCHAR", "VARCHAR", "passenger_phone_trunc"),
  ("passenger_id::VARCHAR", "VARCHAR", "passenger_id"),
  ("passenger_travelpass_name::VARCHAR", "VARCHAR", "passenger_travelpass_name"),
  ("passenger_travelpass_user_id::VARCHAR", "VARCHAR", "passenger_travelpass_user_id"),
  ("passenger_over_18::BOOLEAN", "BOOLEAN", "passenger_over_18"),
  ("passenger_seats::INTEGER", "INTEGER", "passenger_seats"),
  ("passenger_contribution::INTEGER", "INTEGER", "passenger_contribution"),
  ("passenger_payments::TEXT", "TEXT", "passenger_payments"),
  ("operator_incentives_sirets::VARCHAR[]", "VARCHAR[]", "operator_incentives_sirets"),
  ("operator_incentives_amount_total::INTEGER", "INTEGER", "operator_incentives_amount_total"),
  ("policy_id::VARCHAR", "VARCHAR", "policy_id"),
  ("policy_incentives_amount_total::INTEGER", "INTEGER", "policy_incentives_amount_total"),
  ("policy_incentives_result_total::INTEGER", "INTEGER", "policy_incentives_result_total"),
  ("fraud_status::VARCHAR", "VARCHAR", "fraud_status"),
  ("fraud_labels::VARCHAR[]", "VARCHAR[]", "fraud_labels"),
  ("anomaly_status::VARCHAR", "VARCHAR", "anomaly_status"),
  ("anomaly_labels::VARCHAR[]", "VARCHAR[]", "anomaly_labels"),
  ("acquisition_status::VARCHAR", "VARCHAR", "acquisition_status"),
  ("status_updated_at::TIMESTAMP", "TIMESTAMP", "status_updated_at"),
  ("final_acquisition_status::VARCHAR", "VARCHAR", "final_acquisition_status"),
  ("valid_acquisition_status::BOOLEAN", "BOOLEAN", "valid_acquisition_status"),
  ("uuid::VARCHAR", "VARCHAR", "uuid"),
  ("legacy_id", "BIGINT", "legacy_id")  
]

@model(
    "archive_zone.archive_journeys_2024",
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
    tags=["archive","journeys_2024"],
)
def execute(context: ExecutionContext, **kwargs):
    from utils.export_data import build_select_query, export_query_to_file
    # Génération de la query
    query = build_select_query(COLUMNS_TYPES, "trusted_zone.journeys_2024")
    output_file = "/tmp/journeys_2024.parquet"
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
        columns=COLUMNS_TYPES,
        output_path=output_file,
        format=file_format,
        chunksize=chunksize,
    )

    # -----------------------------
    # Upload vers S3
    # -----------------------------
    upload_info = upload_to_s3(
        file_path=output_file,
        key=f"exports/journeys_2024.{file_format.lower()}",
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
