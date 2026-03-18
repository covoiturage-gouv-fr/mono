MODEL (
  name raw_zone.campaign_incentives_2024,
  kind INCREMENTAL_BY_UNIQUE_KEY (
    unique_key (_id)
  ),
  gateway duckdb,
  grain [_id],
  tags ['raw', 'campaign', 'incentives', '2024'],
);

SELECT * FROM read_parquet('s3://geo-datasets-archives/exports/incentives_2024.parquet');

