MODEL (
  name trusted_zone.journeys_2024,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column start_datetime,
    lookback 1
  ),
  start '2024-01-01',
  end '2024-12-31',
  grain '_id',
  tags ['trusted', 'journeys', '2024'],
);

JINJA_QUERY_BEGIN;
{{ journeys_model_generator("@start_ds", "@end_ds") }}
JINJA_END;

CREATE INDEX IF NOT EXISTS journeys_2024_id_index ON @this_model USING btree (_id);