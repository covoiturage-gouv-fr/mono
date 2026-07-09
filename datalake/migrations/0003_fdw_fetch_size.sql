-- FDW scan tuning — the datalake reads its sources through postgres_fdw foreign
-- tables (schema dlk_import). The default fetch_size is 100 rows PER network
-- round-trip, which starves full-history scans: a cross-DB join pulling millions
-- of rows dies by round-trips (measured: a 3.5k-row model took 14× longer than a
-- 1.3M-row one, purely on round-trip latency).
--
--   fetch_size=50000      → 500× fewer round-trips on wide scans (10–100× wall time).
--   use_remote_estimate   → planner asks the remote for real row/cost estimates
--                           instead of guessing, so cross-DB join order / pushdown
--                           is chosen correctly.
--
-- The FDW SERVER itself is created by ops (OpenTofu, see 0000_fdw). These are
-- performance OPTIONS on that server, which its owner (the datalake role) may set —
-- owned here so a fresh/staging DB gets tuned FDW scans without a manual step. If
-- the team prefers, this can move to the OpenTofu server definition instead.
--
-- Idempotent: looks up the single postgres_fdw server dynamically and ADDs the
-- option if absent, else SETs it (safe across environments and re-runs).
DO $$
DECLARE
  srv  text;
  opts text[];
BEGIN
  SELECT s.srvname, s.srvoptions
    INTO srv, opts
  FROM pg_foreign_server s
  JOIN pg_foreign_data_wrapper w ON w.oid = s.srvfdw
  WHERE w.fdwname = 'postgres_fdw'
  ORDER BY s.srvname
  LIMIT 1;

  IF srv IS NULL THEN
    RAISE NOTICE 'no postgres_fdw server found — skipping FDW tuning';
    RETURN;
  END IF;

  IF EXISTS (SELECT 1 FROM unnest(opts) o WHERE o LIKE 'fetch_size=%') THEN
    EXECUTE format('ALTER SERVER %I OPTIONS (SET fetch_size %L)', srv, '50000');
  ELSE
    EXECUTE format('ALTER SERVER %I OPTIONS (ADD fetch_size %L)', srv, '50000');
  END IF;

  IF EXISTS (SELECT 1 FROM unnest(opts) o WHERE o LIKE 'use_remote_estimate=%') THEN
    EXECUTE format('ALTER SERVER %I OPTIONS (SET use_remote_estimate %L)', srv, 'true');
  ELSE
    EXECUTE format('ALTER SERVER %I OPTIONS (ADD use_remote_estimate %L)', srv, 'true');
  END IF;
END $$;
