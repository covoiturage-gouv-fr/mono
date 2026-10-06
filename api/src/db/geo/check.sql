-- Garde-fou, sans modification : joué seul (`import.sh check`, avec report.sql) ou juste avant
-- import.sql (`import.sh apply`, même transaction). Un dernier millésime plus ancien ou tronqué
-- casserait le géocodage.
DO $$
DECLARE
  _latest smallint := (SELECT max(year) FROM geo.perimeters);
  _latest_rows bigint := (SELECT count(*) FROM geo.perimeters WHERE year = _latest);
  _new_latest smallint := (SELECT max(year) FROM geo_export.perimeters);
  _new_rows bigint := (SELECT count(*) FROM geo_export.perimeters WHERE year = _new_latest);
BEGIN
  IF _new_latest IS NULL THEN
    RAISE EXCEPTION 'geo_export.perimeters est vide';
  END IF;

  IF _new_latest < _latest OR _new_rows < _latest_rows * 0.9 THEN
    RAISE EXCEPTION 'millésime % (% lignes) refusé : millésime en service % (% lignes)',
      _new_latest, _new_rows, _latest, _latest_rows;
  END IF;

  IF NOT EXISTS (SELECT 1 FROM geo_export.com_evolution) THEN
    RAISE EXCEPTION 'geo_export.com_evolution est vide';
  END IF;

  RAISE NOTICE 'en service : millésime % (% lignes) ; import : millésime % (% lignes)',
    _latest, _latest_rows, _new_latest, _new_rows;
END;
$$;
