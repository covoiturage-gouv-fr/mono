MODEL (
  name trusted_zone.journeys_latest,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column start_datetime,
    lookback 1,
    batch_size 30
  ),
  start '2026-01-01 00:00:00+0100',
  end 'now()',
  grain '_id',
  tags ['trusted', 'journeys', 'latest'],
);
JINJA_QUERY_BEGIN;
{{ journeys_model_generator("@start_ts", "@end_ts") }}
JINJA_END;

@create_index(@this_model, _id, 'name=journeys_latest_id_index');
@create_index(@this_model, start_datetime_tz, 'name=journeys_latest_start_datetime_tz_index');
@create_index(@this_model, start_h3_index, 'name=journeys_latest_start_h3_index_index');
@create_index(@this_model, end_h3_index, 'name=journeys_latest_end_h3_index_index');
