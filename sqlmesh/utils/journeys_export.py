from utils.upload import upload_to_s3, s3_file_exists

COLUMNS_TYPES = [
    ("_id", "INTEGER", "_id"),
    ("uuid", "VARCHAR", "uuid"),
    ("legacy_id", "BIGINT", "legacy_id"),
    ("created_at", "TIMESTAMP", "created_at"),
    ("updated_at", "TIMESTAMP", "updated_at"),
    ("operator_id", "INTEGER", "operator_id"),
    ("operator_name", "VARCHAR", "operator_name"),
    ("operator_siret", "VARCHAR", "operator_siret"),
    ("operator_journey_id", "VARCHAR", "operator_journey_id"),
    ("operator_trip_id", "VARCHAR", "operator_trip_id"),
    ("operator_class", "VARCHAR", "operator_class"),
    ("start_datetime", "TIMESTAMP", "start_datetime"),
    ("start_datetime_tz", "TIMESTAMP", "start_datetime_tz"),
    ("start_position_x", "REAL", "start_position_x"),
    ("start_position_y", "REAL", "start_position_y"),
    ("start_h3_index::VARCHAR", "VARCHAR", "start_h3_index"),
    ("start_geo_code", "VARCHAR", "start_geo_code"),
    ("end_datetime", "TIMESTAMP", "end_datetime"),
    ("end_datetime_tz", "TIMESTAMP", "end_datetime_tz"),
    ("end_position_x", "REAL", "end_position_x"),
    ("end_position_y", "REAL", "end_position_y"),
    ("end_h3_index::VARCHAR", "VARCHAR", "end_h3_index"),
    ("end_geo_code", "VARCHAR", "end_geo_code"),
    ("geo_errors::TEXT", "TEXT", "geo_errors"),
    ("geo_updated_at", "TIMESTAMP", "geo_updated_at"),
    ("distance", "INTEGER", "distance"),
    ("duration", "INTEGER", "duration"),
    ("licence_plate", "VARCHAR", "licence_plate"),
    ("driver_identity_key", "VARCHAR", "driver_identity_key"),
    ("driver_operator_user_id", "VARCHAR", "driver_operator_user_id"),
    ("driver_phone", "VARCHAR", "driver_phone"),
    ("driver_phone_trunc", "VARCHAR", "driver_phone_trunc"),
    ("driver_id", "VARCHAR", "driver_id"),
    ("driver_travelpass_name", "VARCHAR", "driver_travelpass_name"),
    ("driver_travelpass_user_id", "VARCHAR", "driver_travelpass_user_id"),
    ("driver_revenue", "INTEGER", "driver_revenue"),
    ("passenger_identity_key", "VARCHAR", "passenger_identity_key"),
    ("passenger_operator_user_id", "VARCHAR", "passenger_operator_user_id"),
    ("passenger_phone", "VARCHAR", "passenger_phone"),
    ("passenger_phone_trunc", "VARCHAR", "passenger_phone_trunc"),
    ("passenger_id", "VARCHAR", "passenger_id"),
    ("passenger_travelpass_name", "VARCHAR", "passenger_travelpass_name"),
    ("passenger_travelpass_user_id", "VARCHAR", "passenger_travelpass_user_id"),
    ("passenger_over_18", "BOOLEAN", "passenger_over_18"),
    ("passenger_seats", "INTEGER", "passenger_seats"),
    ("passenger_contribution", "INTEGER", "passenger_contribution"),
    ("passenger_payments::TEXT", "TEXT", "passenger_payments"),
    ("fraud_status", "VARCHAR", "fraud_status"),
    ("fraud_labels::VARCHAR[]", "VARCHAR[]", "fraud_labels"),
    ("anomaly_status", "VARCHAR", "anomaly_status"),
    ("anomaly_labels::VARCHAR[]", "VARCHAR[]", "anomaly_labels"),
    ("acquisition_status", "VARCHAR", "acquisition_status"),
    ("status_updated_at", "TIMESTAMP", "status_updated_at"),
    ("final_acquisition_status", "BOOLEAN", "final_acquisition_status"),
    ("valid_acquisition_status", "BOOLEAN", "valid_acquisition_status"),
]

MODEL_COLUMNS = {
    "status": "VARCHAR",
    "bucket": "VARCHAR",
    "key": "VARCHAR",
    "format": "VARCHAR",
    "size_bytes": "BIGINT",
    "rows": "BIGINT",
    "columns": "INTEGER",
    "date_uploaded": "TIMESTAMP",
}


def build_and_upload_journeys_year(context, year: int) -> dict | None:
    """Export journeys for a given year to parquet and upload to S3.

    Returns a result dict, or None if the file already exists in S3.
    """
    from utils.export_data import build_select_query, export_query_to_file

    file_format = "parquet"
    chunksize = 100_000
    conn = context.engine_adapter.connection

    s3_key = f"exports/journeys_{year}.{file_format}"
    if s3_file_exists(s3_key):
        print(f"--- Skipping {year}: s3://{s3_key} already exists ---")
        return None

    query = build_select_query(COLUMNS_TYPES, f"archive_zone.journeys_{year}")
    output_file = f"/tmp/journeys_{year}.parquet"

    print(f"--- Exporting {year} ---")
    export_info = export_query_to_file(
        conn=conn,
        query=query,
        columns=COLUMNS_TYPES,
        output_path=output_file,
        format=file_format,
        chunksize=chunksize,
    )

    upload_info = upload_to_s3(file_path=output_file, key=s3_key)

    return {
        "status": "uploaded",
        "bucket": upload_info["bucket"],
        "key": upload_info["key"],
        "format": upload_info["format"],
        "size_bytes": upload_info["size_bytes"],
        "rows": export_info["rows"],
        "columns": export_info["columns"],
        "date_uploaded": upload_info["date_uploaded"],
    }
