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
  WHERE datetime >= @start_ts::timestamp - INTERVAL '1 day' 
  AND datetime < @end_ts::timestamp + INTERVAL '1 day'
) != (
  SELECT COUNT(*) FROM @this_model
  WHERE datetime >= @start_ts::timestamp - INTERVAL '1 day' 
  AND datetime < @end_ts::timestamp + INTERVAL '1 day'
);
