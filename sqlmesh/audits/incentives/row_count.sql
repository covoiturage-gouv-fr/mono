-- Audit: Row count match for campaign incentives
-- Compares row counts between policy.incentives (PG) and the target model.
-- Returns a row when counts differ (causing the audit to fail).

-- Direction: PG → Raw
-- Attach to: raw_zone.incentives_* models
AUDIT (
  name assert_incentives_row_count_pg_to_raw,
  blocking true
);

SELECT 1 AS failure
WHERE (
  SELECT COUNT(*)
  FROM policy.incentives
  WHERE datetime BETWEEN @start_ts AND @end_ts
) != (
  SELECT COUNT(*) FROM @this_model
);
