MODEL (
  name archive_zone.journeys_2023,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column start_datetime,
    batch_size 30,
    lookback 1
  ),
  start '2023-01-01 00:00:00+0100',
  end   '2023-12-31 23:59:59+0100',
  grain '_id',
  tags ['archive', 'carpools', 'journeys', '2023'],
);

JINJA_QUERY_BEGIN;
{{ journeys_model_generator("@start_ts", "@end_ts") }}
JINJA_END;

