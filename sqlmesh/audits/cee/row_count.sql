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
  WHERE datetime >= @start_ts::timestamp
    AND datetime <  @end_ts::timestamp
) != (
  SELECT COUNT(*) FROM @this_model
);
