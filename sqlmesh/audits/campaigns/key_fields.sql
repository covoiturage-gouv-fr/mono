-- Audit: Key field consistency for campaign incentives
-- For rows that exist in both source and target, verifies critical columns match.
-- Checked fields: policy_id/id, amount, status

-- Direction: PG → Raw
-- Attach to: raw_zone.incentives_* models
AUDIT (
  name assert_incentives_key_fields_pg_to_raw,
  blocking false
);

SELECT pi._id
FROM policy.incentives pi
INNER JOIN @this_model t ON t._id = pi._id
WHERE pi.datetime BETWEEN @start_ts AND @end_ts
  AND (
    pi.policy_id    IS DISTINCT FROM t.id
    OR pi.amount    IS DISTINCT FROM t.amount
    OR pi.status::VARCHAR IS DISTINCT FROM t.status
  );
