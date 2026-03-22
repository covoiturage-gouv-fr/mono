MODEL (
  name raw_zone.cee_applications,
  kind FULL,
  grain [_id],
  tags ['raw', 'cee'],
  audits (
    assert_cee_row_count_pg_to_raw,
    assert_cee_missing_rows_pg_to_raw,
    assert_cee_key_fields_pg_to_raw,
  ),
  columns (
    _id BIGINT,
    carpool_v2_id BIGINT,
    operator_id BIGINT,
    operator_journey_id VARCHAR,
    datetime TIMESTAMP,
    journey_type VARCHAR,
    is_specific BOOLEAN,
    application_timestamp TIMESTAMP,
    created_at TIMESTAMP,
    updated_at TIMESTAMP,
  )
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
  AND cv2_journey.operator_journey_id = cee.operator_journey_id;
