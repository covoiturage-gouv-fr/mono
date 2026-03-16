MODEL (
  name archive_zone.carpools_to_journeys_2024,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column start_datetime,
    batch_size 30,
    lookback 1
  ),
  start '2024-01-01 00:00:00+0100',
  end   '2024-12-31 23:59:59+0100',
  grain '_id',
  tags ['archive', 'carpools', 'journeys', '2024'],
);

JINJA_QUERY_BEGIN;
{{ journeys_model_generator("@start_ts", "@end_ts") }}
JINJA_END;

