MODEL (
  name refined_zone.obs_directions_day_latest,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column journey_date,
    lookback 3,
    batch_size 1,
  ),
  start '2026-01-01 00:00:00+0100',
  grain ['code', 'type', 'journey_date', 'direction'],
  tags  ['refined', 'observatoire', 'directions_day', 'latest'],
);
JINJA_QUERY_BEGIN;
{{ obs_directions_day_generator("trusted_zone.journeys_latest", "@start_ts", "@end_ts") }}
JINJA_END;

@create_unique_index(@this_model, code, type, journey_date, direction);
@create_index(@this_model, journey_date, 'name=obs_directions_day_latest_date_index');