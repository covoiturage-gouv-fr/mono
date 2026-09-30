-- Joué par `just geo-import` dans une seule transaction, après restauration de geo_export.
-- geo_export.perimeters (millésimes exportés du datalake) remplace geo.perimeters. Le premier import
-- garde l'ancienne table en geo.perimeters_legacy (sauvegarde, plus jamais lue).
-- Garde-fou, avant tout travail : un dernier millésime plus ancien ou tronqué casserait le géocodage.
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
END;
$$;

-- Index et stats construits avant le remplacement : SET SCHEMA les emporte, et le verrou exclusif
-- sur geo.perimeters ne dure que le temps des renommages (le géocodage de l'acquisition le lit).
-- Noms explicites : ceux par défaut (perimeters_pkey, perimeters_com_idx) existent sur la table legacy.
ALTER TABLE geo_export.perimeters ADD CONSTRAINT perimeters_dlk_pkey PRIMARY KEY (id);
CREATE INDEX perimeters_dlk_year_idx ON geo_export.perimeters USING btree (year);
CREATE INDEX perimeters_dlk_arr_idx ON geo_export.perimeters USING btree (arr);
CREATE INDEX perimeters_dlk_com_idx ON geo_export.perimeters USING btree (com);
CREATE INDEX perimeters_dlk_epci_idx ON geo_export.perimeters USING btree (epci);
CREATE INDEX perimeters_dlk_aom_idx ON geo_export.perimeters USING btree (aom);
CREATE INDEX perimeters_dlk_dep_idx ON geo_export.perimeters USING btree (dep);
CREATE INDEX perimeters_dlk_reg_idx ON geo_export.perimeters USING btree (reg);
CREATE INDEX perimeters_dlk_country_idx ON geo_export.perimeters USING btree (country);
CREATE INDEX perimeters_dlk_surface_idx ON geo_export.perimeters USING btree (surface);
CREATE INDEX perimeters_dlk_centroid_idx ON geo_export.perimeters USING gist (centroid);
CREATE INDEX perimeters_dlk_geom_idx ON geo_export.perimeters USING gist (geom);
CREATE INDEX perimeters_dlk_geom_simple_idx ON geo_export.perimeters USING gist (geom_simple);
ANALYZE geo_export.perimeters;

DO $$
BEGIN
  IF to_regclass('geo.perimeters_legacy') IS NULL THEN
    ALTER TABLE geo.perimeters RENAME TO perimeters_legacy;
  ELSE
    DROP TABLE geo.perimeters;
  END IF;

  ALTER TABLE geo_export.perimeters SET SCHEMA geo;
END;
$$;

-- com_evolution, sans versions : remplace les années couvertes par l'export (depuis 2020) ; la prod
-- garde les mouvements antérieurs (depuis 2019).
DELETE FROM geo.com_evolution
WHERE year >= (SELECT min(year) FROM geo_export.com_evolution);

INSERT INTO geo.com_evolution (year, mod, old_com, new_com, l_mod)
SELECT year, mod, old_com, new_com, l_mod FROM geo_export.com_evolution;
