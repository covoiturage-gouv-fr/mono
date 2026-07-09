{{ config(
    materialized='view',
    tags=['exposed', 'observatory', 'campaigns']
) }}

-- Campagnes d'incitation exposées à l'API (remplace la lecture directe de
-- `raw_zone.campaigns` + jointure `trusted_zone.perimeters_agg` faite côté API Deno).
--
-- L'API ne lit que la zone exposée. Cette vue :
--   * projette UNIQUEMENT les colonnes du contrat public (celles servies par l'ancien
--     `raw_zone.campaigns` de sqlmesh) — on écarte les colonnes internes du seed brut
--     (email, collectivite, column00, « Mise à jour », nom_région, siren…) qui ne
--     doivent PAS fuiter sur une API publique ;
--   * renomme `lien_page_collectivité` -> `lien` (contrat historique) ;
--   * embarque la géométrie GeoJSON du territoire (jointure périmètre au dernier millésime).

WITH latest_millesime AS (
    SELECT max(year) AS year FROM {{ ref('perimeters_agg') }}
)

SELECT
    a.type,
    a.code,
    a.premiere_campagne,
    a.budget_incitations,
    a.date_debut,
    a.date_fin,
    a.conducteur_montant_max_par_passager,
    a.conducteur_montant_max_par_mois,
    a.conducteur_montant_min_par_passager,
    a.conducteur_trajets_max_par_mois,
    a.passager_trajets_max_par_mois,
    a.passager_gratuite,
    a.passager_eligible_gratuite,
    a.passager_reduction_ticket,
    a.passager_eligibilite_reduction,
    a.passager_montant_ticket,
    a.zone_sens_des_trajets,
    a.zone_exclusion,
    a.si_zone_exclue_liste,
    a.autre_exclusion,
    a.trajet_longueur_min,
    a.trajet_longueur_max,
    a.trajet_classe_de_preuve,
    a.operateurs,
    a.autres_informations,
    a."lien_page_collectivité" AS lien,
    ST_AsGeoJSON(b.geom, 6)::jsonb AS geom
FROM {{ source('raw', 'campaigns') }} AS a
LEFT JOIN {{ ref('perimeters_agg') }} AS b
    ON a.type = b.type
    AND left(a.code, 9) = b.code
    AND b.year = (SELECT year FROM latest_millesime)
