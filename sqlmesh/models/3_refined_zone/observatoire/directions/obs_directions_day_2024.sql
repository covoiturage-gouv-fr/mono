MODEL (
  name refined_zone.obs_directions_day_2024,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column journey_date,
    batch_size 1,
    batch_concurrency 1,
  ),
  start '2024-01-01 00:00:00+0100',
  end '2024-12-31 23:59:59+0100',
  grain ['code', 'type', 'journey_date', 'direction'],
  tags  ['refined', 'observatoire', 'directions_day', '2024'],
);
JINJA_QUERY_BEGIN;
{{ obs_directions_day_generator("trusted_zone.journeys_2024", "@start_ts", "@end_ts") }}
JINJA_END;

@create_unique_index(@this_model, code, type, journey_date, direction);
@create_index(@this_model, journey_date, 'name=obs_directions_day_2024_date_index');