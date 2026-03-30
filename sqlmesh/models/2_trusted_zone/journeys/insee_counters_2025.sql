MODEL (
  name trusted_zone.insee_counters_2025,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column start_datetime,
    batch_size 31,
  ),
  start '2025-01-01 00:00:00+0100',
  end '2025-12-31 23:59:59+0100',
  grain '_id',
  tags ['trusted', 'insee_counters', '2025'],
);

JINJA_QUERY_BEGIN;
{{ insee_counters_model_generator("trusted_zone.journeys_2025", "@start_ts", "@end_ts") }}
JINJA_END;

@create_index(@this_model, _id, 'name=insee_counters_2025_id_index');
@create_index(@this_model, start_datetime, 'name=insee_counters_2025_start_datetime_index');
