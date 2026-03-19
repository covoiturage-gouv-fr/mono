MODEL (
  name archive_zone.cee_applications,
  kind FULL,
  grain '_id',
  tags ['archive', 'cee'],
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

-- Resolution via v1 carpool_id:
--   cee.carpool_id → carpool.carpools._id → acquisition_id → carpool_v2.carpools.legacy_id
LEFT JOIN carpool.carpools cv1
  ON cee.carpool_id IS NOT NULL
  AND cv1._id = cee.carpool_id
LEFT JOIN carpool_v2.carpools cv2_id
  ON cv1._id IS NOT NULL
  AND cv2_id.legacy_id = cv1.acquisition_id

-- Fallback resolution via operator_id + operator_journey_id when carpool_id is absent
LEFT JOIN carpool_v2.carpools cv2_journey
  ON cee.carpool_id IS NULL
  AND cv2_journey.operator_id = cee.operator_id
  AND cv2_journey.operator_journey_id = cee.operator_journey_id
;

