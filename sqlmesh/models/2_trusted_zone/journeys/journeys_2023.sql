MODEL (
  name trusted_zone.journeys_2023,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column start_datetime,
    lookback 1
  ),
  start '2023-01-01',
  end '2023-12-31',
  grain '_id',
  tags ['trusted', 'journeys', '2023'],
);

JINJA_QUERY_BEGIN;
{{ journeys_model_generator("@start_ds", "@end_ds") }}
JINJA_END;

CREATE INDEX IF NOT EXISTS journeys_2023_id_index ON @this_model USING btree (_id);