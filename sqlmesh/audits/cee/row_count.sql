-- Audit: Row count match for CEE applications
-- Compares row counts between source and target.
-- Returns a row when counts differ (causing the audit to fail).

-- Direction: PG → Archive
-- Attach to: archive_zone.cee_applications
AUDIT (
  name assert_cee_row_count_pg_to_archive,
  blocking true
);

SELECT 1 AS failure
WHERE (
  SELECT COUNT(*) FROM cee.cee_applications
) != (
  SELECT COUNT(*) FROM @this_model
);
