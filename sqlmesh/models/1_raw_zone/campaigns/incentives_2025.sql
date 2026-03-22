MODEL (
  name raw_zone.incentives_2025,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column datetime,
    lookback 3,
    batch_size 30,
  ),
  start '2025-01-01 00:00:00+0100',
  end '2025-12-31 23:59:59+0100',
  grain '_id',
  tags ['raw', 'campaign', 'incentives', '2025'],
  audits (
    assert_incentives_complete,
    assert_incentives_row_count_pg_to_raw,
    assert_incentives_missing_rows_pg_to_raw,
    assert_incentives_key_fields_pg_to_raw,
  ),
);
JINJA_QUERY_BEGIN;
{{ incentives_model_generator("@start_ts", "@end_ts") }}
JINJA_END;

@create_index(@this_model, carpool_v2_id, 'name=incentives_2025_carpool_v2_id_index');
