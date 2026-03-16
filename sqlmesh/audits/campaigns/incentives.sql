-- Audit: assert_campaign_incentives_complete
-- Checks that every record in policy.incentives for the evaluated time window
-- has been normalised and is present in raw_zone.campaign_incentives.
-- Non-blocking: reports missing rows without halting the pipeline.

AUDIT (
  name assert_campaign_incentives_complete,
  blocking false
);

SELECT pi._id
FROM policy.incentives pi
WHERE pi.datetime BETWEEN @start_ts AND @end_ts
  AND NOT EXISTS (
    SELECT 1
    FROM @this_model ci
    WHERE ci._id = pi._id
  )
