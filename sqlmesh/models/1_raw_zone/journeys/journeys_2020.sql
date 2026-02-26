MODEL (
  name raw_zone.journeys_2020,
  kind FULL,
  grain _id,
  gateway duckdb
);

SELECT * FROM read_parquet('s3://geo-datasets-archives/exports/journeys_2020.parquet');
