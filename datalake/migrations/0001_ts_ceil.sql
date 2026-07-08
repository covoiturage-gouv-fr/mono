-- ts_ceil(timestamp, seconds): round a timestamp UP to the nearest N-second bucket.
-- Used by exposed/export models (export_partners, export_opendata). Historically a
-- prod-only DB function defined nowhere in the repo; owned here so a fresh datalake
-- DB can build the export models.
CREATE OR REPLACE FUNCTION public.ts_ceil(ts timestamp without time zone, secs integer)
RETURNS timestamp without time zone
LANGUAGE sql IMMUTABLE AS $$
  SELECT 'epoch'::timestamp + (ceil(extract(epoch FROM ts) / secs) * secs) * interval '1 second';
$$;
