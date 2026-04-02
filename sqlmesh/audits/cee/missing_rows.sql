-- Audit: Missing rows for CEE applications
-- Finds rows present in source but absent in target (by _id).
-- Any returned row means the audit fails.

-- Direction: PG → Raw
-- Attach to: raw_zone.cee_applications
AUDIT (
  name assert_cee_missing_rows_pg_to_raw,
  blocking true
);

SELECT cee._id
FROM cee.cee_applications cee
WHERE cee.datetime BETWEEN @start_ts AND @end_ts
  AND NOT EXISTS (
    SELECT 1 FROM @this_model t 
    WHERE t._id = cee._id
  );
