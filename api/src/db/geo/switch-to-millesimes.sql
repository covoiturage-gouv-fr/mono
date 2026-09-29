-- Bascule unique, jouée par `just geo-import` dans la transaction de l'import (sans effet ensuite) :
-- la table geo.perimeters devient geo.perimeters_legacy (sauvegarde), geo.perimeters une vue sur le
-- dernier millésime de perimeters_all, et les fonctions qui lisaient geo.perimeters (lookups par
-- année : campagnes, APDF) lisent perimeters_all.
DO $$
DECLARE
  f oid;
BEGIN
  IF (SELECT relkind FROM pg_class WHERE oid = 'geo.perimeters'::regclass) = 'v' THEN
    RETURN;
  END IF;

  IF NOT EXISTS (SELECT 1 FROM geo.perimeters_all) THEN
    RAISE EXCEPTION 'aucun millésime dans geo.perimeters_all';
  END IF;

  ALTER TABLE geo.perimeters RENAME TO perimeters_legacy;

  CREATE VIEW geo.perimeters AS
    SELECT * FROM geo.perimeters_all
    WHERE year = (SELECT max(year) FROM geo.perimeters_all);

  -- Réécriture dynamique : couvre toutes les versions présentes en base (geo.*, territory.*,
  -- observatoire legacy). attach_millesime vise geo.perimeters exprès (millésime en service).
  FOR f IN
    SELECT p.oid
    FROM pg_proc p
    JOIN pg_namespace n ON n.oid = p.pronamespace
    WHERE n.nspname NOT IN ('pg_catalog', 'information_schema')
      AND p.prokind IN ('f', 'p')
      AND p.prosrc ~ 'geo\.perimeters\M'
      AND p.oid <> 'geo.attach_millesime'::regproc
  LOOP
    EXECUTE regexp_replace(pg_get_functiondef(f), 'geo\.perimeters\M', 'geo.perimeters_all', 'g');
  END LOOP;
END;
$$;
