-- FDW datalake : conditions des campagnes publiées (carte de l'observatoire, open data).
-- Seules les campagnes validées et non supprimées sortent ; ni auteur ni validateur (identifiants internes).
-- Côté datalake, import manuel dans dlk_import une fois l'API déployée (voir datalake/README.md).

CREATE OR REPLACE VIEW dlk_export.policy_campaign_conditions AS
SELECT
  _id,
  territory_type,
  territory_code,
  territory_label,
  operator_ids,
  premiere_campagne,
  date_debut,
  date_fin,
  budget_incitations,
  conducteur_montant_min_par_passager,
  conducteur_montant_max_par_passager,
  conducteur_montant_max_par_mois,
  conducteur_trajets_max_par_mois,
  passager_gratuite,
  passager_eligible_gratuite,
  passager_reduction_ticket,
  passager_eligibilite_reduction,
  passager_montant_ticket,
  passager_trajets_max_par_mois,
  zone_sens_des_trajets,
  zone_exclusion,
  zone_exclusion_liste,
  autre_exclusion,
  trajet_longueur_min,
  trajet_longueur_max,
  trajet_classe_preuve,
  lien_page_collectivite,
  autres_informations,
  validated_at,
  updated_at
FROM policy.campaign_conditions
WHERE status = 'validated' AND deleted_at IS NULL;

-- Les droits par défaut ne couvrent que les vues créées par leur propriétaire (vnm).
DO $$
BEGIN
  IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'datalake_fdw') THEN
    GRANT SELECT ON dlk_export.policy_campaign_conditions TO datalake_fdw;
  END IF;
END
$$;
