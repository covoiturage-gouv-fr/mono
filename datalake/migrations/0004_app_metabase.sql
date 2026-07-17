-- =============================================================================
-- 0004_app_metabase — schéma cible Metabase + droits datalake_mb_ro (GEN-575)
-- =============================================================================
--
-- Metabase est découplé de covoiturage_production : ses dashboards ne liront plus
-- que des modèles dbt matérialisés dans le schéma `app_metabase`, via le rôle en
-- lecture seule `datalake_mb_ro` (créé par l'infra, carte A — CONNECT + preset
-- « readonly » Scaleway, mot de passe dans Secret Manager).
--
-- Cette migration est jouée par le rôle `datalake` (propriétaire des schémas dbt) :
--   1. crée le schéma app_metabase (owner datalake) ;
--   2. ouvre USAGE + un DEFAULT PRIVILEGE SELECT à datalake_mb_ro, posé AVANT que
--      dbt matérialise quoi que ce soit → chaque table/vue créée ensuite par
--      datalake hérite du SELECT, sans GRANT rétroactif ;
--   3. retire l'accès large que le preset readonly a ouvert sur les zones dbt
--      (datalake_mb_ro ne doit voir QUE app_metabase).
--
-- Idempotent : IF NOT EXISTS + GRANT/REVOKE rejouables. Le preset Scaleway est
-- posé en `ignore_changes` côté OpenTofu : ces REVOKE ne seront pas réconciliés.
--
-- Périmètre : on ne touche QUE ce que le rôle datalake a granté lui-même (USAGE +
-- SELECT table sur les zones). L'USAGE sur `public` a été granté par
-- _rdb_superadmin sur un schéma qu'il possède : datalake ne peut pas le révoquer.
-- Si l'on veut fermer `public` à Metabase, c'est à faire côté infra (OpenTofu).
--
-- Les grants sont gardés par un test d'existence du rôle : sur un environnement
-- neuf/staging où l'infra n'a pas encore créé datalake_mb_ro, la migration crée le
-- schéma et n'échoue pas (comme 0003 pour un serveur FDW absent).
-- -----------------------------------------------------------------------------

-- 1. Schéma cible (owner = datalake, le rôle qui joue cette migration et dbt)
CREATE SCHEMA IF NOT EXISTS app_metabase AUTHORIZATION datalake;

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'datalake_mb_ro') THEN
    RAISE NOTICE 'role datalake_mb_ro absent — schéma créé, droits différés (infra carte A)';
    RETURN;
  END IF;

  -- 2. Droits sur app_metabase — posés AVANT tout modèle dbt.
  GRANT USAGE ON SCHEMA app_metabase TO datalake_mb_ro;
  -- objets déjà présents (no-op sur un schéma vide, utile aux rejeux ultérieurs)
  GRANT SELECT ON ALL TABLES IN SCHEMA app_metabase TO datalake_mb_ro;
  -- objets FUTURS créés par datalake dans ce schéma → SELECT automatique
  ALTER DEFAULT PRIVILEGES FOR ROLE datalake IN SCHEMA app_metabase
    GRANT SELECT ON TABLES TO datalake_mb_ro;

  -- 3. Fermeture des zones dbt (le preset readonly Scaleway est trop large).
  REVOKE USAGE ON SCHEMA zone_raw, zone_trusted, zone_aggregated, zone_exposed, dlk_import
    FROM datalake_mb_ro;
  REVOKE ALL ON ALL TABLES IN SCHEMA zone_raw, zone_trusted, zone_aggregated, zone_exposed, dlk_import
    FROM datalake_mb_ro;
END $$;
