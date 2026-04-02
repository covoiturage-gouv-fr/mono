-- Audit: Row count match for CEE applications
-- Compares row counts between source and target.
-- Returns a row when counts differ (causing the audit to fail).

-- Direction: PG → Raw
-- Attach to: raw_zone.cee_applications
AUDIT (
  name assert_cee_row_count_pg_to_raw,
  blocking true
);

SELECT 1 AS failure
WHERE (
  SELECT COUNT(*)
  FROM cee.cee_applications
  WHERE datetime >= @start_ts::timestamp - INTERVAL '1 day'
    AND datetime <  @end_ts::timestamp + INTERVAL '1 day'
) != (
  SELECT COUNT(*) FROM @this_model
  WHERE datetime >= @start_ts::timestamp - INTERVAL '1 day'
    AND datetime <  @end_ts::timestamp + INTERVAL '1 day'
);
