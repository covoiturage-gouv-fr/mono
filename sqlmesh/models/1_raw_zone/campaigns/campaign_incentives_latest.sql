MODEL (
  name raw_zone.campaign_incentives_latest,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column datetime,
  ),
  start '2026-01-01 00:00:00+0100',
  grain '_id',
  tags ['archive', 'campaign', 'incentives', 'latest'],
);

@campaign_incentives(@start_ts, @end_ts);

