-- Audit: Row count match for journeys
-- Compares row counts between source and target for the same time window.
-- Returns a row when counts differ (causing the audit to fail).

-- Direction: PG → Raw
-- Attach to: raw_zone.journeys
AUDIT (
  name assert_journeys_row_count_pg_to_raw,
  blocking true
);

SELECT 1 AS failure
WHERE (
  SELECT COUNT(*)
  FROM carpool_v2.carpools
  WHERE start_datetime BETWEEN @start_ts AND @end_ts
) != (
  SELECT COUNT(*) FROM @this_model
);
