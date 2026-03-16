MODEL (
  name archive_zone.campaign_incentives_2025,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column datetime,
    batch_size 30,
    lookback 1,
  ),
  start '2025-01-01 00:00:00+0100',
  end   '2025-12-31 23:59:59+0100',
  grain '_id',
  tags ['archive', 'campaign', 'incentives', '2025'],
);

JINJA_QUERY_BEGIN;
{{ campaign_incentives("@start_ts", "@end_ts") }}
JINJA_END;

