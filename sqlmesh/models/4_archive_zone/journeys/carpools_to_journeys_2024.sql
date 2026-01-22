MODEL (
  name archive_zone.carpools_to_journeys_2024,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column start_datetime,
    lookback 1
  ),
  start '2024-01-01',
  end '2025-01-01',
  grain '_id',
  tags ['archive', 'carpools', 'journeys_2024'],
);
JINJA_QUERY_BEGIN;
{{ journeys_model_generator("@start_ds", "@end_ds") }}
JINJA_END;

CREATE INDEX IF NOT EXISTS journeys_latest_id_index ON @this_model USING btree (_id);