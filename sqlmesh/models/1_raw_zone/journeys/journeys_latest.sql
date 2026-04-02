MODEL (
  name raw_zone.journeys_latest,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column start_datetime,
    lookback 3,
    batch_size 30
  ),
  start '2026-01-01 00:00:00+0100',
  grain ['_id', 'fraud_status', 'anomaly_status', 'acquisition_status'],
  tags ['raw', 'journeys', 'latest'],
  audits (
    assert_journeys_key_fields_pg_to_raw,
  ),
);
JINJA_QUERY_BEGIN;
{{ journeys_model_generator("@start_ts", "@end_ts") }}
JINJA_END;

@create_index(@this_model, _id, 'name=journeys_latest_id_index');
@create_index(@this_model, start_datetime, 'name=journeys_latest_start_datetime_index');
