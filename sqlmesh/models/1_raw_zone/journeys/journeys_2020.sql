MODEL (
  name raw_zone.journeys_2020,
  kind INCREMENTAL_BY_UNIQUE_KEY (
    unique_key (_id)
  ),
  gateway duckdb,
  grain [_id],
  tags ['raw', 'journeys', '2020'],
);

SELECT * FROM read_parquet('s3://geo-datasets-archives/exports/journeys_2020.parquet');
