-- =============================================================================
-- 0008_operators_strict_perimeters — operators_* passe en mode strict
-- =============================================================================
--
-- operators_model appelait filtered_carpools sans strict, contrairement à territory et
-- fraud : operators_*_plm_* gardait les codes hors Paris / Lyon / Marseille (doublons de
-- clés dans les vues operators_*_com_*, union arr + plm) et operators_*_aom_* comptait
-- les AOM régionales.
--
-- Le mode strict ne change que ces lignes-là (mêmes règles intra et même filtre géo pour
-- les autres codes) : on les supprime plutôt que de tout recalculer depuis les trajets.
--
-- Idempotent.
-- -----------------------------------------------------------------------------

DO $$
DECLARE
  tbl record;
BEGIN
  FOR tbl IN
    SELECT table_name
    FROM information_schema.tables
    WHERE table_schema = 'zone_aggregated'
      AND table_type = 'BASE TABLE'
      AND table_name ~ '^operators_(day|month|quarter|semester|year)_plm_(both|from|to)$'
  LOOP
    EXECUTE format(
      'DELETE FROM zone_aggregated.%I WHERE code NOT IN (''75056'', ''69123'', ''13055'')',
      tbl.table_name
    );
  END LOOP;

  FOR tbl IN
    SELECT table_name
    FROM information_schema.tables
    WHERE table_schema = 'zone_aggregated'
      AND table_type = 'BASE TABLE'
      AND table_name ~ '^operators_(day|month|quarter|semester|year)_aom_(both|from|to)$'
  LOOP
    EXECUTE format(
      'DELETE FROM zone_aggregated.%I WHERE code IN (SELECT aom FROM zone_trusted.aom_region)',
      tbl.table_name
    );
  END LOOP;
END $$;
