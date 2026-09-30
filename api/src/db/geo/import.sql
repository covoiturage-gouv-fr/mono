-- Joué par `just geo-import` dans une seule transaction, après restauration de geo_export.
-- geo_export.perimeters (millésimes exportés du datalake) remplace geo.perimeters. Le premier import
-- garde l'ancienne table en geo.perimeters_legacy (sauvegarde, plus jamais lue).
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

  -- Garde-fou : un dernier millésime plus ancien ou tronqué casserait le géocodage.
  IF _new_latest < _latest OR _new_rows < _latest_rows * 0.9 THEN
    RAISE EXCEPTION 'millésime % (% lignes) refusé : millésime en service % (% lignes)',
      _new_latest, _new_rows, _latest, _latest_rows;
  END IF;

  IF to_regclass('geo.perimeters_legacy') IS NULL THEN
    ALTER TABLE geo.perimeters RENAME TO perimeters_legacy;
  ELSE
    DROP TABLE geo.perimeters;
  END IF;

  ALTER TABLE geo_export.perimeters SET SCHEMA geo;
END;
$$;

ALTER TABLE geo.perimeters ADD PRIMARY KEY (id);
CREATE INDEX ON geo.perimeters USING btree (year);
CREATE INDEX ON geo.perimeters USING btree (arr);
CREATE INDEX ON geo.perimeters USING btree (com);
CREATE INDEX ON geo.perimeters USING btree (epci);
CREATE INDEX ON geo.perimeters USING btree (aom);
CREATE INDEX ON geo.perimeters USING btree (dep);
CREATE INDEX ON geo.perimeters USING btree (reg);
CREATE INDEX ON geo.perimeters USING btree (country);
CREATE INDEX ON geo.perimeters USING btree (surface);
CREATE INDEX ON geo.perimeters USING gist (centroid);
CREATE INDEX ON geo.perimeters USING gist (geom);
CREATE INDEX ON geo.perimeters USING gist (geom_simple);
ANALYZE geo.perimeters;

-- com_evolution, sans versions : remplace les années couvertes par l'export (depuis 2020) ; la prod
-- garde les mouvements antérieurs (depuis 2019).
DELETE FROM geo.com_evolution
WHERE year >= (SELECT min(year) FROM geo_export.com_evolution);

INSERT INTO geo.com_evolution (year, mod, old_com, new_com, l_mod)
SELECT year, mod, old_com, new_com, l_mod FROM geo_export.com_evolution;
