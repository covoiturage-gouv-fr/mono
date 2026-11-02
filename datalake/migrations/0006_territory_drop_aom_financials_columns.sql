-- =============================================================================
-- 0006_territory_drop_aom_financials_columns — colonnes sorties de territory_*
-- =============================================================================
--
-- Les stats financières AOM (self/other, intra/inter, quantiles de distance) ont
-- quitté le générateur commun territory_* pour les modèles dédiés aom_financials_*.
-- dbt (on_schema_change='append_new_columns') ne supprime pas de colonne : on les
-- retire ici des tables territory_* existantes, sinon elles gardent des valeurs
-- figées que plus rien ne recalcule.
--
-- CASCADE emporte les vues qui les lisent (territory_*_com_* et app_metabase.mb_aom_*) :
-- elles sont recréées par le dbt run suivant (pipeline-daily), à lancer juste après
-- le déploiement.
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
      AND c.table_name LIKE 'territory\_%'
      AND t.table_type = 'BASE TABLE'
      AND c.column_name IN (
        'driver_revenue_intra', 'driver_revenue_inter',
        'passenger_contribution_intra', 'passenger_contribution_inter',
        'distance_intra', 'distance_inter',
        'mean_distance', 'median_distance', 'q1_distance', 'q3_distance',
        'oi_operator_only',
        'oi_amount_operator_intra', 'oi_amount_operator_inter',
        'oi_amount_total_intra', 'oi_amount_total_inter',
        'oi_amount_collectivite_self', 'oi_amount_collectivite_self_intra',
        'oi_amount_collectivite_self_inter', 'oi_amount_collectivite_other',
        'ci_amount_total_self', 'ci_amount_total_other',
        'ci_result_total_self', 'ci_result_total_other',
        'carpools_collectivite_self_incentive', 'carpools_collectivite_other_incentive',
        'carpools_operator_incentive', 'carpools_other_incentive'
      )
  LOOP
    EXECUTE format(
      'ALTER TABLE zone_aggregated.%I DROP COLUMN IF EXISTS %I CASCADE',
      col.table_name, col.column_name
    );
  END LOOP;
END $$;
