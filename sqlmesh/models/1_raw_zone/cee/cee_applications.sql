MODEL (
  name raw_zone.cee_applications,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column datetime,
    lookback 3,
    batch_size 30,
  ),
  start '2020-01-01 00:00:00+0100',
  end '2025-06-01 00:00:00+0200',
  cron '@yearly',
  grain [_id],
  tags ['raw', 'cee'],
  audits (
    assert_cee_row_count_pg_to_raw,
    assert_cee_missing_rows_pg_to_raw,
    assert_cee_key_fields_pg_to_raw,
  ),
);

SELECT
  cee._id,
  COALESCE(cv2_id._id, cv2_journey._id) AS carpool_v2_id,
  cee.operator_id,
  cee.operator_journey_id,
  cee.datetime,
  cee.journey_type::VARCHAR AS journey_type,
  cee.is_specific,
  cee.application_timestamp,
  cee.created_at,
  cee.updated_at
FROM cee.cee_applications cee
LEFT JOIN carpool.carpools cv1
  ON cee.carpool_id IS NOT NULL AND cv1._id = cee.carpool_id
LEFT JOIN carpool_v2.carpools cv2_id
  ON cv1._id IS NOT NULL AND cv2_id.legacy_id = cv1.acquisition_id
LEFT JOIN carpool_v2.carpools cv2_journey
  ON cee.carpool_id IS NULL
  AND cv2_journey.operator_id = cee.operator_id
  AND cv2_journey.operator_journey_id = cee.operator_journey_id
WHERE cee.datetime >= @start_ts
  AND cee.datetime < @end_ts;

@IF(@runtime_stage = 'creating', CREATE INDEX IF NOT EXISTS cee_applications_v2_id ON raw_zone.cee_applications (carpool_v2_id));
