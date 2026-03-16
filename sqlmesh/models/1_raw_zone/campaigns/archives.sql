MODEL (
  name raw_zone.campaign_incentives_archives,
  kind INCREMENTAL_BY_UNIQUE_KEY (
    unique_key (_id)
  ),
  gateway duckdb,
  grain [_id],
  tags ['raw', 'campaign', 'incentives'],
--  audits (assert_campaign_incentives_complete),
);

SELECT * FROM read_parquet('s3://geo-datasets-archives/exports/incentives_2019.parquet')
UNION
SELECT * FROM read_parquet('s3://geo-datasets-archives/exports/incentives_2020.parquet')
UNION
SELECT * FROM read_parquet('s3://geo-datasets-archives/exports/incentives_2021.parquet')
UNION
SELECT * FROM read_parquet('s3://geo-datasets-archives/exports/incentives_2022.parquet')
UNION
SELECT * FROM read_parquet('s3://geo-datasets-archives/exports/incentives_2023.parquet')
UNION
SELECT * FROM read_parquet('s3://geo-datasets-archives/exports/incentives_2024.parquet')
UNION
SELECT * FROM read_parquet('s3://geo-datasets-archives/exports/incentives_2025.parquet')

ORDER BY datetime
;

