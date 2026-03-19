MODEL (
  name trusted_zone.journeys_2022,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column start_datetime,
    batch_size 30
  ),
  start '2022-01-01 00:00:00+0100',
  end '2022-12-31 23:59:59+0100',
  grain '_id',
  tags ['trusted', 'journeys', '2022'],
);

JINJA_QUERY_BEGIN;
{{ trusted_journeys_model_generator("raw_zone.journeys_2022", "@start_ts", "@end_ts") }}
JINJA_END;

@create_index(@this_model, _id, 'name=journeys_2022_id_index');
@create_index(@this_model, start_datetime_tz, 'name=journeys_2022_start_datetime_tz_index');
@create_index(@this_model, start_h3_index, 'name=journeys_2022_start_h3_index_index');
@create_index(@this_model, end_h3_index, 'name=journeys_2022_end_h3_index_index');
