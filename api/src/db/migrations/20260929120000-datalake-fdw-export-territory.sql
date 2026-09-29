-- FDW datalake : tout le schéma territory (voir 20260703000000-datalake-fdw-export.sql).
-- Côté datalake, `just fdw-sync` importe ces vues dans dlk_import une fois l'API déployée.

CREATE OR REPLACE VIEW dlk_export.territory_territory_group_selector AS
SELECT territory_group_id, selector_type, selector_value
FROM territory.territory_group_selector;

CREATE OR REPLACE VIEW dlk_export.territory_territory_perimeters AS
SELECT _id, territory_id, version, arr, valid_from, valid_to, created_at
FROM territory.territory_perimeters;

-- Les droits par défaut ne couvrent que les vues créées par leur propriétaire (vnm).
DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'datalake_fdw') THEN
    GRANT SELECT ON dlk_export.territory_territory_group_selector TO datalake_fdw;
    GRANT SELECT ON dlk_export.territory_territory_perimeters TO datalake_fdw;
  END IF;
END
$$;
