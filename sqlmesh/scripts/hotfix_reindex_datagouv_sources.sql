-- =============================================================================
-- One-off PRODUCTION hot-fix: restore missing indexes on the live
-- trusted_zone.journeys and trusted_zone.insee_counters snapshots.
-- =============================================================================
--
-- WHY
--   A fixed index-name + `CREATE INDEX IF NOT EXISTS` regression left the live
--   snapshots un-indexed (PostgreSQL index names are unique per schema, so the
--   post-statement was silently skipped while the previous snapshot still owned
--   the name). refined_zone.export_opendata_list (the data.gouv export) then
--   does full ~50 GB seq scans and the export stalls.
--
--   This script reindexes the CURRENT snapshots so the export runs immediately.
--   The `@index_name` macro shipped in this PR prevents the regression on every
--   future snapshot rebuild.
--
-- HOW TO RUN
--   * Use the owner / a superuser role.
--   * Run with autocommit ON (psql default). CREATE INDEX CONCURRENTLY must NOT
--     run inside a transaction block (no BEGIN/COMMIT, no -1 / --single-transaction).
--   * CONCURRENTLY avoids write locks; on ~50 GB expect several minutes per index.
--
-- -----------------------------------------------------------------------------
-- STEP 1 - confirm the live physical snapshot behind each logical view.
--          The hashes below were current at authoring time; re-run this and
--          substitute the returned names into STEP 2 if they differ.
-- -----------------------------------------------------------------------------
SELECT v.relname            AS logical_view,
       src.relname          AS live_snapshot
FROM pg_depend d
JOIN pg_rewrite r ON r.oid = d.objid
JOIN pg_class   v ON v.oid = r.ev_class
JOIN pg_class src ON src.oid = d.refobjid AND src.relkind = 'r'
WHERE v.relnamespace = 'trusted_zone'::regnamespace
  AND v.relname IN ('journeys', 'insee_counters')
  AND src.relname LIKE v.relname || '\_\_%'
ORDER BY 1;

-- -----------------------------------------------------------------------------
-- STEP 2 - create the missing indexes on the live snapshots.
--          Names follow the new `<base>_<snapshot_suffix>` convention so they
--          will not collide with other snapshots.
-- -----------------------------------------------------------------------------

-- trusted_zone.journeys  (live snapshot: journeys__2404643179)
CREATE INDEX CONCURRENTLY IF NOT EXISTS journeys_id_index_2404643179
  ON trusted_zone.journeys__2404643179 (_id);
CREATE INDEX CONCURRENTLY IF NOT EXISTS journeys_start_datetime_index_2404643179
  ON trusted_zone.journeys__2404643179 (start_datetime);
CREATE INDEX CONCURRENTLY IF NOT EXISTS journeys_start_datetime_tz_index_2404643179
  ON trusted_zone.journeys__2404643179 (start_datetime_tz);
CREATE INDEX CONCURRENTLY IF NOT EXISTS journeys_start_h3_idx_2404643179
  ON trusted_zone.journeys__2404643179 (start_h3_index);
CREATE INDEX CONCURRENTLY IF NOT EXISTS journeys_end_h3_idx_2404643179
  ON trusted_zone.journeys__2404643179 (end_h3_index);

-- trusted_zone.insee_counters  (live snapshot: insee_counters__3095982230)
CREATE INDEX CONCURRENTLY IF NOT EXISTS insee_counters_id_index_3095982230
  ON trusted_zone.insee_counters__3095982230 (_id);
CREATE INDEX CONCURRENTLY IF NOT EXISTS insee_counters_start_datetime_index_3095982230
  ON trusted_zone.insee_counters__3095982230 (start_datetime);

-- -----------------------------------------------------------------------------
-- STEP 3 - verify and re-run the export.
-- -----------------------------------------------------------------------------
-- SELECT tablename, indexname FROM pg_indexes
-- WHERE schemaname = 'trusted_zone'
--   AND tablename IN ('journeys__2404643179', 'insee_counters__3095982230')
-- ORDER BY 1, 2;
--
-- Then: deno task ... export:datagouv -s 2026-05-01 -e 2026-06-01
