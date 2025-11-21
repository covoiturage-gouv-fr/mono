MODEL (
  name trusted_zone.journeys_2021,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column start_datetime,
    lookback 1
  ),
  start '2021-01-01',
  end '2021-12-31',
  grain '_id',
  tags ['trusted', 'journeys', '2021'],
);

JINJA_QUERY_BEGIN;
{{ journeys_model_generator("@start_ds", "@end_ds") }}
JINJA_END;

CREATE INDEX IF NOT EXISTS journeys_2021_id_index ON @this_model USING btree (_id);