MODEL (
  name archive_zone.campaign_incentives_2019,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column datetime,
    batch_size 30,
    lookback 1
  ),
  start '2019-01-01 00:00:00+0100',
  end   '2019-12-31 23:59:59+0100',
  grain '_id',
  tags ['archive', 'stage-1'],
  audits (
    unique_values(columns := (_id)),
    -- assert_campaign_incentives_row_count_pg_to_archive,
    -- assert_campaign_incentives_missing_rows_pg_to_archive,
    -- assert_campaign_incentives_key_fields_pg_to_archive,
  ),
);

@campaign_incentives(@start_ts, @end_ts);
