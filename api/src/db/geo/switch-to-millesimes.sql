-- Bascule unique, jouée par `just geo-import` dans la transaction de l'import (sans effet ensuite) :
-- la table geo.perimeters devient geo.perimeters_legacy (sauvegarde, plus jamais lue) et la table
-- partitionnée geo.perimeters_all prend le nom geo.perimeters. Le code et les fonctions geo.*
-- continuent de lire geo.perimeters, tous millésimes, sans modification.
DO $$
BEGIN
  IF to_regclass('geo.perimeters_all') IS NULL THEN
    RETURN;
  END IF;

  IF NOT EXISTS (SELECT 1 FROM geo.perimeters_all) THEN
    RAISE EXCEPTION 'aucun millésime dans geo.perimeters_all';
  END IF;

  ALTER TABLE geo.perimeters RENAME TO perimeters_legacy;
  ALTER TABLE geo.perimeters_all RENAME TO perimeters;
END;
$$;
