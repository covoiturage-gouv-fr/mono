MODEL (
  name raw_zone.campaign_incentives_latest,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column datetime,
    lookback 3,
    batch_size 30,
  ),
  start '2026-01-01 00:00:00+0100',
  grain '_id',
  tags ['raw', 'campaign', 'incentives', 'latest'],
  audits (
    assert_campaign_incentives_complete,
    assert_campaign_incentives_row_count_pg_to_raw,
    assert_campaign_incentives_missing_rows_pg_to_raw,
    assert_campaign_incentives_key_fields_pg_to_raw,
  ),
);

@campaign_incentives(@start_ts, @end_ts);

