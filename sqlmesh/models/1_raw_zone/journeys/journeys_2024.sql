MODEL (
  name raw_zone.journeys_2024,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column start_datetime,
    lookback 3,
    batch_size 30
  ),
  start '2024-01-01 00:00:00+0100',
  end '2024-12-31 23:59:59+0100',
  grain ['_id', 'fraud_status', 'anomaly_status', 'acquisition_status'],
  tags ['raw', 'journeys', '2024'],
  audits (
    assert_journeys_row_count_pg_to_raw,
    assert_journeys_missing_rows_pg_to_raw,
    assert_journeys_key_fields_pg_to_raw,
  ),
);
JINJA_QUERY_BEGIN;
{{ journeys_model_generator("@start_ts", "@end_ts") }}
JINJA_END;

@create_index(@this_model, _id, 'name=journeys_2024_id_index');
@create_index(@this_model, start_datetime_tz, 'name=journeys_2024_start_datetime_tz_index');
@create_index(@this_model, start_h3_index, 'name=journeys_2024_start_h3_index_index');
@create_index(@this_model, end_h3_index, 'name=journeys_2024_end_h3_index_index');
