-- Joué par `import.sh apply` dans la transaction ouverte par check.sql, après restauration de geo_export.
-- geo_export.perimeters (millésimes exportés du datalake) remplace geo.perimeters. Le premier import
-- garde l'ancienne table en geo.perimeters_legacy (sauvegarde, plus jamais lue) ; les suivants gardent
-- la table remplacée en geo.perimeters_prev (retour arrière = deux RENAME).

-- Le géocodage lit geo.perimeters en continu : si une requête longue tient la table, on abandonne
-- plutôt que de mettre toute l'API en file derrière le verrou exclusif.
SET LOCAL lock_timeout = '30s';

-- Index et stats construits avant le remplacement : SET SCHEMA les emporte, et le verrou exclusif
-- sur geo.perimeters ne dure que le temps des renommages.
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

-- com_evolution, sans versions : remplace les années couvertes par l'export (depuis 2020) ; la prod
-- garde les mouvements antérieurs (depuis 2019). Avant le swap, pour ne pas allonger le verrou.
DELETE FROM geo.com_evolution
WHERE year >= (SELECT min(year) FROM geo_export.com_evolution);

INSERT INTO geo.com_evolution (year, mod, old_com, new_com, l_mod)
SELECT year, mod, old_com, new_com, l_mod FROM geo_export.com_evolution;

DO $$
DECLARE
  _idx text;
BEGIN
  IF to_regclass('geo.perimeters_legacy') IS NULL THEN
    ALTER TABLE geo.perimeters RENAME TO perimeters_legacy;
  ELSE
    DROP TABLE IF EXISTS geo.perimeters_prev;
    -- La table remplacée garde ses index perimeters_dlk_* : les renommer libère les noms pour la nouvelle.
    FOR _idx IN
      SELECT c.relname FROM pg_index i
      JOIN pg_class c ON c.oid = i.indexrelid
      WHERE i.indrelid = 'geo.perimeters'::regclass
    LOOP
      EXECUTE format('ALTER INDEX geo.%I RENAME TO %I', _idx, replace(_idx, 'perimeters_dlk_', 'perimeters_prev_'));
    END LOOP;
    ALTER TABLE geo.perimeters RENAME TO perimeters_prev;
  END IF;

  ALTER TABLE geo_export.perimeters SET SCHEMA geo;
END;
$$;
