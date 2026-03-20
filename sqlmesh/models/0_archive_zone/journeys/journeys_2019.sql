MODEL (
  name archive_zone.journeys_2019,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column start_datetime,
    batch_size 30,
    lookback 1
  ),
  start '2019-01-01 00:00:00+0100',
  end   '2019-12-31 23:59:59+0100',
  grain '_id',
  tags ['archive', 'stage-1'],
  audits (
    assert_journeys_row_count_pg_to_archive,
    assert_journeys_missing_rows_pg_to_archive,
    assert_journeys_key_fields_pg_to_archive,
  ),
);

JINJA_QUERY_BEGIN;
{{ journeys_model_generator("@start_ts", "@end_ts") }}
JINJA_END;

