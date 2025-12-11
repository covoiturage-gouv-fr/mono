CREATE OR REPLACE FUNCTION ts_ceil(ts timestamp, step integer)
  RETURNS timestamp AS $$
  BEGIN
    RETURN to_timestamp(ceil(extract(epoch from ts) / step) * step);
  END;
$$ LANGUAGE plpgsql IMMUTABLE;
