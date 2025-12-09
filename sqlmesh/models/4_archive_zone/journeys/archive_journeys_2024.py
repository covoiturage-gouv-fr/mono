import pandas as pd
from sqlmesh import ExecutionContext, model
from utils.upload import upload_to_s3
from utils.export import export_query_to_file  # la fonction que nous avons adaptée

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

    columns = [
      "_id::BIGINT AS _id",
      "created_at::TIMESTAMP AS created_at",
      "updated_at::TIMESTAMP AS updated_at",
      "operator_id::BIGINT AS operator_id",
      "operator_name::VARCHAR AS operator_name",
      "operator_siret::VARCHAR AS operator_siret",
      "operator_journey_id::VARCHAR AS operator_journey_id",
      "operator_trip_id::VARCHAR AS operator_trip_id",
      "operator_class::VARCHAR AS operator_class",
      "start_datetime::TIMESTAMP AS start_datetime",
      "start_datetime_tz::TIMESTAMP AS start_datetime_tz",
      "ST_AsEWKB(start_position::geometry)::BYTEA AS start_position",
      "start_h3_index::VARCHAR AS start_h3_index",
      "start_geo_code::VARCHAR AS start_geo_code",
      "end_datetime::TIMESTAMP AS end_datetime",
      "end_datetime_tz::TIMESTAMP AS end_datetime_tz",
      "ST_AsEWKB(end_position::geometry)::BYTEA AS end_position",
      "end_h3_index::VARCHAR AS end_h3_index",
      "end_geo_code::VARCHAR AS end_geo_code",
      "geo_errors::TEXT AS geo_errors",
      "geo_updated_at::TIMESTAMP AS geo_updated_at",
      "distance::INTEGER AS distance",
      "EXTRACT(EPOCH FROM duration)::BIGINT AS duration",
      "licence_plate::VARCHAR AS licence_plate",
      "driver_identity_key::VARCHAR AS driver_identity_key",
      "driver_operator_user_id::VARCHAR AS driver_operator_user_id",
      "driver_phone::VARCHAR AS driver_phone",
      "driver_phone_trunc::VARCHAR AS driver_phone_trunc",
      "driver_id::VARCHAR AS driver_id",
      "driver_travelpass_name::VARCHAR AS driver_travelpass_name",
      "driver_travelpass_user_id::VARCHAR AS driver_travelpass_user_id",
      "driver_revenue::INTEGER AS driver_revenue",
      "passenger_identity_key::VARCHAR AS passenger_identity_key",
      "passenger_operator_user_id::VARCHAR AS passenger_operator_user_id",
      "passenger_phone::VARCHAR AS passenger_phone",
      "passenger_phone_trunc::VARCHAR AS passenger_phone_trunc",
      "passenger_id::VARCHAR AS passenger_id",
      "passenger_travelpass_name::VARCHAR AS passenger_travelpass_name",
      "passenger_travelpass_user_id::VARCHAR AS passenger_travelpass_user_id",
      "passenger_over_18::BOOLEAN AS passenger_over_18",
      "passenger_seats::INTEGER AS passenger_seats",
      "passenger_contribution::INTEGER AS passenger_contribution",
      "passenger_payments::TEXT AS passenger_payments",
      "operator_incentives_sirets::VARCHAR[] AS operator_incentives_sirets",
      "operator_incentives_amount_total::INTEGER AS operator_incentives_amount_total",
      "policy_id::VARCHAR AS policy_id",
      "policy_incentives_amount_total::INTEGER AS policy_incentives_amount_total",
      "policy_incentives_result_total::INTEGER AS policy_incentives_result_total",
      "fraud_status::VARCHAR AS fraud_status",
      "fraud_labels::VARCHAR[] AS fraud_labels",
      "anomaly_status::VARCHAR AS anomaly_status",
      "anomaly_labels::VARCHAR[] AS anomaly_labels",
      "acquisition_status::VARCHAR AS acquisition_status",
      "status_updated_at::TIMESTAMP AS status_updated_at",
      "final_acquisition_status::VARCHAR AS final_acquisition_status",
      "valid_acquisition_status::BOOLEAN AS valid_acquisition_status",
      '"uuid"::VARCHAR AS "uuid"',
      "legacy_id::BIGINT AS legacy_id"
    ]

    # Génération de la query
    query = "SELECT " + ", ".join(columns) + " FROM trusted_zone.journeys_2024"
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
