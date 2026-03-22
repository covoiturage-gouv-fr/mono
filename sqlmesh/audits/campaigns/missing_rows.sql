-- Audit: Missing rows for campaign incentives
-- Finds rows present in policy.incentives but absent in the target model.
-- Any returned row means the audit fails.

-- Direction: PG → Raw
-- Attach to: raw_zone.incentives_* models
AUDIT (
  name assert_incentives_missing_rows_pg_to_raw,
  blocking true
);

SELECT pi._id
FROM policy.incentives pi
WHERE pi.datetime BETWEEN @start_ts AND @end_ts
  AND NOT EXISTS (
    SELECT 1 FROM @this_model t WHERE t._id = pi._id
  );
