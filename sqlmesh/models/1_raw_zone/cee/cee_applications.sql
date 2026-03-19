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

SELECT * FROM read_parquet('s3://geo-datasets-archives/exports/cee_applications.parquet');
