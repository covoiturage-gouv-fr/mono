-- Audit: Key field consistency for campaign incentives
-- For rows that exist in both source and target, verifies critical columns match.
-- Checked fields: policy_id/campaign_id, amount, status
--
-- Note: carpool_v2_id requires a join chain (policy.incentives → carpool.carpools
-- → carpool_v2.carpools) so is checked via the resolved value in the model.

-- Direction: PG → Raw (via campaign_incentives_latest)
-- Attach to: raw_zone.campaign_incentives_latest
AUDIT (
  name assert_campaign_incentives_key_fields_pg_to_raw,
  blocking false
);

SELECT pi._id
FROM policy.incentives pi
INNER JOIN @this_model t ON t._id = pi._id
WHERE pi.datetime BETWEEN @start_ts AND @end_ts
  AND (
    pi.policy_id    IS DISTINCT FROM t.campaign_id
    OR pi.amount    IS DISTINCT FROM t.amount
    OR pi.status::VARCHAR IS DISTINCT FROM t.status
  );
