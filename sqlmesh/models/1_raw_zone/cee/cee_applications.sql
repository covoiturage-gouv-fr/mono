MODEL (
  name raw_zone.cee_applications,
  kind FULL,
  gateway duckdb,
  grain [uuid],
  tags ['raw', 'cee'],
  columns (
    uuid UUID,
    carpool_v2_id BIGINT,
    operator_id BIGINT,
    operator_journey_id VARCHAR,
    datetime TIMESTAMP,
    journey_type VARCHAR,
    is_specific BOOLEAN,
    application_timestamp TIMESTAMP,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
  )
);

SELECT
  uuid::UUID                       AS uuid,
  carpool_v2_id::BIGINT            AS carpool_v2_id,
  operator_id::BIGINT              AS operator_id,
  operator_journey_id::VARCHAR     AS operator_journey_id,
  datetime::TIMESTAMP              AS datetime,
  journey_type::VARCHAR            AS journey_type,
  is_specific::BOOLEAN             AS is_specific,
  application_timestamp::TIMESTAMP AS application_timestamp,
  created_at::TIMESTAMP            AS created_at,
  updated_at::TIMESTAMP            AS updated_at
FROM read_parquet('s3://geo-datasets-archives/exports/cee_applications.parquet');
