MODEL (
  name raw_zone.cee_applications,
  kind FULL,
  gateway duckdb,
  grain [_id],
  tags ['raw', 'cee'],
);

SELECT * FROM read_parquet('s3://geo-datasets-archives/exports/cee_applications.parquet');
