-- =============================================================================
-- 0007_long_grains_drop_unique_users — unique_* sortis des grains longs operators / od
-- =============================================================================
--
-- Les grains longs (quarter, semester, year) somment désormais les modèles mensuels
-- (rollup_model). unique_drivers / unique_passengers ne se somment pas et ne sont
-- recalculés que pour territory_* : on les retire des tables operators_* et od_* de ces
-- grains plutôt que d'y laisser des valeurs figées que plus rien ne recalcule.
--
-- CASCADE emporte les vues *_com_* qui les lisent : elles sont recréées par le dbt run
-- suivant (pipeline-daily), à lancer juste après le déploiement.
--
-- Idempotent : ne cible que les colonnes encore présentes.
-- -----------------------------------------------------------------------------

DO $$
DECLARE
  col record;
BEGIN
  FOR col IN
    SELECT c.table_name, c.column_name
    FROM information_schema.columns AS c
    INNER JOIN information_schema.tables AS t USING (table_schema, table_name)
    WHERE c.table_schema = 'zone_aggregated'
      AND c.table_name ~ '^(operators|od)_(quarter|semester|year)_'
      AND t.table_type = 'BASE TABLE'
      AND c.column_name IN ('unique_drivers', 'unique_passengers')
  LOOP
    EXECUTE format(
      'ALTER TABLE zone_aggregated.%I DROP COLUMN IF EXISTS %I CASCADE',
      col.table_name, col.column_name
    );
  END LOOP;
END $$;
