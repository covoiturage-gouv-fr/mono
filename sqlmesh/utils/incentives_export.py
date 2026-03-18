from datetime import datetime
from zoneinfo import ZoneInfo
from utils.upload import upload_to_s3, s3_file_exists

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

MODEL_COLUMNS = {
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
}


def build_incentives_direct_query(start_ts: str, end_ts: str) -> str:
    """Build the incentives query directly from source tables for a given date range.

    Mirrors the campaign_incentives Jinja macro but without an intermediate model,
    avoiding data duplication between archive_zone and raw_zone.
    """
    return f"""
WITH ni AS (
  SELECT
    pi._id,
    c2._id AS carpool_v2_id,
    c2.operator_id,
    c2.operator_journey_id
  FROM policy.incentives pi
  LEFT JOIN carpool.carpools c1 ON pi.carpool_id = c1._id
  LEFT JOIN carpool_v2.carpools c2 ON c1.acquisition_id = c2.legacy_id
  WHERE pi.carpool_id IS NOT NULL
    AND pi.datetime BETWEEN '{start_ts}' AND '{end_ts}'

  UNION

  SELECT
    pi._id,
    c2._id AS carpool_v2_id,
    pi.operator_id,
    pi.operator_journey_id
  FROM policy.incentives pi
  LEFT JOIN carpool_v2.carpools c2
    ON pi.operator_id = c2.operator_id AND pi.operator_journey_id = c2.operator_journey_id
  WHERE pi.operator_id IS NOT NULL AND pi.operator_journey_id IS NOT NULL
    AND pi.datetime BETWEEN '{start_ts}' AND '{end_ts}'
)

SELECT
  pi._id::BIGINT                  AS _id,
  ni.carpool_v2_id::BIGINT        AS carpool_v2_id,
  pi.datetime::TIMESTAMP          AS datetime,
  ni.operator_id::BIGINT          AS operator_id,
  ni.operator_journey_id::VARCHAR AS operator_journey_id,
  pi.policy_id::BIGINT            AS campaign_id,
  pp.name::VARCHAR                AS campaign_name,
  ccp.siret::VARCHAR              AS territory_siret,
  ttg.name::VARCHAR               AS territory_name,
  pi.amount::INTEGER              AS amount,
  pi.result::INTEGER              AS result,
  pi.status::VARCHAR              AS status,
  pi.state::VARCHAR               AS state

FROM policy.incentives pi
JOIN ni                                  ON pi._id = ni._id
LEFT JOIN policy.policies pp             ON pi.policy_id    = pp._id
LEFT JOIN territory.territory_group ttg  ON pp.territory_id = ttg._id
LEFT JOIN company.companies ccp          ON ttg.company_id  = ccp._id

ORDER BY pi.datetime
"""


def build_and_upload_incentives_year(context, year: int) -> dict | None:
    """Export incentives for a given year to parquet and upload to S3.

    Queries policy.incentives and related source tables directly,
    without depending on an intermediate archive SQL model.

    Returns a result dict, or None if the file already exists in S3.
    """
    from utils.export_data import export_query_to_file

    tz = ZoneInfo("Europe/Paris")
    file_format = "parquet"
    chunksize = 100_000
    conn = context.engine_adapter.connection

    start = datetime(year, 1, 1, tzinfo=tz)
    end = datetime(year + 1, 1, 1, tzinfo=tz)
    s3_key = f"exports/incentives_{year}.{file_format}"

    if s3_file_exists(s3_key):
        print(f"--- Skipping {year}: s3://{s3_key} already exists ---")
        return None

    query = build_incentives_direct_query(start.isoformat(), end.isoformat())
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

    upload_info = upload_to_s3(file_path=output_file, key=s3_key)

    return {
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
