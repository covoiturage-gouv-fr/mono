-- Audit: Missing rows for journeys
-- Finds rows present in source but absent in target (by _id).
-- Any returned row means the audit fails.

-- Direction: PG → Raw
-- Attach to: raw_zone.journeys_* models
AUDIT (
  name assert_journeys_missing_rows_pg_to_raw,
  blocking true
);

SELECT c._id
FROM carpool_v2.carpools c
WHERE c.start_datetime >= @start_ts::timestamp
  AND c.start_datetime <  @end_ts::timestamp
  AND NOT EXISTS (
    SELECT 1 FROM @this_model t WHERE t._id = c._id
  );
