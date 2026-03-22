-- Audit: Key field consistency for CEE applications
-- For rows that exist in both source and target, verifies critical columns match.
-- Checked fields: operator_id, datetime, journey_type

-- Direction: PG → Raw
-- Attach to: raw_zone.cee_applications
AUDIT (
  name assert_cee_key_fields_pg_to_raw,
  blocking false
);

SELECT cee._id
FROM cee.cee_applications cee
INNER JOIN @this_model t ON t._id = cee._id
WHERE (
  cee.operator_id    IS DISTINCT FROM t.operator_id
  OR cee.datetime    IS DISTINCT FROM t.datetime
  OR cee.journey_type::VARCHAR IS DISTINCT FROM t.journey_type
);
