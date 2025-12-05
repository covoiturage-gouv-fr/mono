MODEL (
  name trusted_zone.journeys_latest,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column start_datetime,
    lookback 1
  ),
  start '2025-01-01',
  end 'now()',
  grain '_id',
  tags ['trusted', 'journeys', 'latest'],
);
JINJA_QUERY_BEGIN;
{{ journeys_model_generator("@start_ds", "@end_ds") }}
JINJA_END;

CREATE INDEX IF NOT EXISTS journeys_latest_id_index ON @this_model USING btree (_id);