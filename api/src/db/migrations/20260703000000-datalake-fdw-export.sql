-- =====================================================================
--  côté API (covoiturage_production) du FDW datalake
-- =====================================================================
--  CHOIX DE CONCEPTION
--    - Vues "security definer" (défaut) : elles tournent avec les droits de leur
--      propriétaire (vnm). `datalake_fdw` n'a AUCUN accès aux schémas applicatifs,
--      seulement USAGE sur dlk_export + SELECT sur les vues.
--    - Enums castés en `text` -> pas besoin de répliquer les types enum via le FDW.
--    - Geometry conservée (start_position/end_position/geo) : utilisée par les
--      exports (ST_X/ST_Y) -> postgis REQUIS aussi côté datalake_production.
--    - PII minimisée : seules les colonnes réellement consommées par les modèles
--      dbt sont exportées (marquées -- PII ci-dessous). Les colonnes PII inutilisées
--      ont été retirées (licence_plate, carpool v1 identity_id, et cee last_name_trunc
--      / phone_trunc / driving_license / identity_key). Restent : téléphones complets
--      (entrée du hash driver_key/passenger_key dans trusted), identity_key et
--      operator_user_id bruts (surfacés dans exposed/export/export_partners).
--    - Projection = colonnes déclarées dans les sources dbt (`models/sources/*.yml`),
--      filtrées sur usage réel. 14 tables : 12 pérennes + 2 temporaires.
--
--  RÉTENTION / RGPD (borne de conservation de la PII exportée)
--  ---------------------------------------------------------------------
--    - Pas de nouvelle collecte : ces vues sont une simple PROJECTION en lecture
--      seule de tables API déjà soumises à la politique de rétention de
--      `covoiturage_production`. Le FDW ne matérialise rien côté API.
--    - Alignement source : `datalake_production` ne conserve PAS la PII au-delà de
--      la source. Les modèles dbt sont reconstruits (full-refresh) depuis les vues ;
--      une ligne purgée/anonymisée côté API disparaît du datalake au rebuild suivant.
--      => la borne de rétention de la PII = celle de covoiturage_production, héritée.
--    - Minimisation par hachage : les PII directes (téléphones, identity_key,
--      operator_user_id) ne servent qu'à dériver `driver_key`/`passenger_key`
--      (md5) dans la couche `trusted` ; la PII brute n'est PAS persistée au-delà
--      de `trusted` (cf. datalake/models/trusted/carpools.sql).
--    - Exception maîtrisée : `identity_key` + `operator_user_id` bruts ne surfacent
--      que dans `exposed/export/export_partners`, export partenaire déjà couvert par
--      les CGU/accords de partage de données opérateurs.
--    - Accès : PII lisible uniquement par le rôle `datalake_fdw` (lecture seule) et
--      le propriétaire des vues ; aucun accès direct aux schémas applicatifs.
--    - TODO (suivi, hors périmètre de cette migration) : acter la borne dans le
--      registre RGPD / AIPD et, si un TTL propre au datalake est décidé, ajouter un
--      job de purge côté `datalake_production`.
-- =====================================================================

CREATE SCHEMA IF NOT EXISTS dlk_export;

-- ============================ carpool_v2 (pérenne) ============================

CREATE OR REPLACE VIEW dlk_export.carpool_v2_carpools AS
SELECT
  _id, created_at, updated_at, operator_id, operator_journey_id, operator_trip_id,
  operator_class, start_datetime, start_position, end_datetime, end_position, distance,
  driver_identity_key,           -- PII
  driver_operator_user_id,       -- PII
  driver_phone,                  -- PII
  driver_phone_trunc,
  driver_travelpass_name, driver_travelpass_user_id, driver_revenue,
  passenger_identity_key,        -- PII
  passenger_operator_user_id,    -- PII
  passenger_phone,               -- PII
  passenger_phone_trunc,
  passenger_travelpass_name, passenger_travelpass_user_id, passenger_over_18,
  passenger_seats, passenger_contribution, passenger_payments, uuid, legacy_id
FROM carpool_v2.carpools;

CREATE OR REPLACE VIEW dlk_export.carpool_v2_geo AS
SELECT _id, carpool_id, updated_at, start_geo_code, end_geo_code, errors
FROM carpool_v2.geo;

CREATE OR REPLACE VIEW dlk_export.carpool_v2_status AS
SELECT
  _id, carpool_id, updated_at,
  acquisition_status::text AS acquisition_status,
  fraud_status::text       AS fraud_status,
  anomaly_status::text     AS anomaly_status
FROM carpool_v2.status;

CREATE OR REPLACE VIEW dlk_export.carpool_v2_operator_incentives AS
SELECT _id, carpool_id, idx, siret, amount
FROM carpool_v2.operator_incentives;

CREATE OR REPLACE VIEW dlk_export.carpool_v2_terms_violation_error_labels AS
SELECT _id, created_at, carpool_id, labels
FROM carpool_v2.terms_violation_error_labels;

-- ============================ policy (pérenne) ============================

CREATE OR REPLACE VIEW dlk_export.policy_incentives AS
SELECT
  _id, policy_id, status::text AS status, meta, carpool_id, amount, datetime,
  result, state::text AS state, operator_id, operator_journey_id
FROM policy.incentives;

CREATE OR REPLACE VIEW dlk_export.policy_policies AS
SELECT
  _id, created_at, updated_at, deleted_at, territory_id, start_date, end_date,
  name, description, unit::text AS unit, status::text AS status, handler,
  incentive_sum, max_amount, tz, descriptive_sheet_url
FROM policy.policies;

-- ============================ operator (pérenne) ============================

CREATE OR REPLACE VIEW dlk_export.operator_operators AS
SELECT
  _id, created_at, updated_at, deleted_at, name, legal_name, siret,
  cgu_accepted_at, cgu_accepted_by, company, address, bank, contacts, uuid, support
FROM operator.operators;

-- ============================ territory (pérenne) ============================

CREATE OR REPLACE VIEW dlk_export.territory_territory_group AS
SELECT _id, company_id, created_at, updated_at, deleted_at, name, shortname, contacts, address
FROM territory.territory_group;

-- ============================ fraudcheck (pérenne) ============================

CREATE OR REPLACE VIEW dlk_export.fraudcheck_labels AS
SELECT _id, created_at, carpool_id, label
FROM fraudcheck.labels;

-- ============================ company (pérenne) ============================

CREATE OR REPLACE VIEW dlk_export.company_companies AS
SELECT
  _id, siret, siren, nic, legal_name, company_naf_code, establishment_naf_code,
  legal_nature_code, legal_nature_label, nonprofit_code, intra_vat,
  geo,                           -- geometry (postgis requis côté datalake)
  address, headquarter, updated_at,
  address_street, address_postcode, address_cedex, address_city
FROM company.companies;

-- ============================ anomaly (pérenne) ============================

CREATE OR REPLACE VIEW dlk_export.anomaly_labels AS
SELECT
  _id, created_at, carpool_id, label, conflicting_carpool_id,
  conflicting_operator_journey_id, overlap_duration_ratio, operator_journey_id
FROM anomaly.labels;

-- ================= carpool v1 (TEMPORAIRE : suppr après consolidation trusted) =================

CREATE OR REPLACE VIEW dlk_export.carpool_carpools AS
SELECT
  _id, created_at, acquisition_id, operator_id, trip_id, operator_trip_id, is_driver,
  operator_class, datetime, duration, start_position, end_position, distance, seats,
  operator_journey_id, cost, meta, status::text AS status,
  start_territory_id, end_territory_id, start_geo_code, end_geo_code, payment
FROM carpool.carpools;

-- ================= cee (TEMPORAIRE : CEE déprécié) =================

CREATE OR REPLACE VIEW dlk_export.cee_cee_applications AS
SELECT
  _id, created_at, updated_at, operator_id, journey_type::text AS journey_type,
  is_specific,
  datetime, carpool_id,
  application_timestamp,
  operator_journey_id
FROM cee.cee_applications;

-- =====================================================================
--  Droits pour datalake_fdw (lecture seule, user créé par OpenTofu PR #36)
-- =====================================================================

GRANT USAGE   ON SCHEMA   dlk_export              TO datalake_fdw;
GRANT SELECT  ON ALL TABLES IN SCHEMA dlk_export  TO datalake_fdw;

-- Auto-grant SELECT sur les futures vues créées par vnm dans dlk_export :
ALTER DEFAULT PRIVILEGES IN SCHEMA dlk_export GRANT SELECT ON TABLES TO datalake_fdw;
