-- Audit: Row count match for campaign incentives
-- Compares row counts between policy.incentives (PG) and the target model.
-- Returns a row when counts differ (causing the audit to fail).
--
-- Note: archive_zone.archive_incentives_* are Python exporters that store
-- only S3 upload metadata, not actual incentive rows. Both audit directions
-- therefore compare against the PG source (policy.incentives).

-- Direction: PG → Raw (via campaign_incentives_latest)
-- Attach to: raw_zone.campaign_incentives_latest
AUDIT (
  name assert_campaign_incentives_row_count_pg_to_raw,
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

-- Direction: PG → Archive
-- Attach to: archive_zone.campaign_incentives_* models
AUDIT (
  name assert_campaign_incentives_row_count_pg_to_archive,
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
