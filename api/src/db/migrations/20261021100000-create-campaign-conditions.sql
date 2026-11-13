-- Conditions des campagnes d'incitation (RPC ou non) : carte de l'observatoire et open data data.gouv.fr.
-- Territoire = couple (type, code) du datalake : beaucoup de collectivités n'ont pas de territory_group.
-- Montants en centimes, distances en km.
CREATE TABLE policy.campaign_conditions (
  _id                                 serial PRIMARY KEY,
  territory_type                      varchar(10) NOT NULL
    CHECK (territory_type IN ('com', 'epci', 'aom', 'dep', 'reg', 'country', 'custom')),
  territory_code                      varchar(20) NOT NULL,
  territory_label                     varchar(256) NOT NULL,
  operator_ids                        int[] NOT NULL CHECK (cardinality(operator_ids) > 0),
  -- auteur : un opérateur, un territoire, ou aucun des deux (équipe RPC)
  created_by_operator_id              int REFERENCES operator.operators(_id),
  created_by_territory_id             int REFERENCES territory.territory_group(_id),
  email_contact                       varchar(256) NOT NULL,
  premiere_campagne                   boolean NOT NULL DEFAULT false,
  date_debut                          date NOT NULL,
  date_fin                            date CHECK (date_fin IS NULL OR date_fin >= date_debut),
  budget_incitations                  int CHECK (budget_incitations >= 0),
  conducteur_montant_min_par_passager int CHECK (conducteur_montant_min_par_passager >= 0),
  conducteur_montant_max_par_passager int CHECK (conducteur_montant_max_par_passager >= 0),
  conducteur_montant_max_par_mois     int CHECK (conducteur_montant_max_par_mois >= 0),
  conducteur_trajets_max_par_mois     int CHECK (conducteur_trajets_max_par_mois >= 0),
  passager_gratuite                   boolean NOT NULL DEFAULT false,
  passager_eligible_gratuite          varchar(20)
    CHECK (passager_eligible_gratuite IN ('tous', 'jeunes_18_25', 'demandeurs_emploi', 'mineurs', 'autre')),
  passager_reduction_ticket           boolean NOT NULL DEFAULT false,
  passager_eligibilite_reduction      varchar(20)
    CHECK (passager_eligibilite_reduction IN ('tous', 'jeunes_18_25', 'demandeurs_emploi', 'mineurs', 'autre')),
  passager_montant_ticket             int CHECK (passager_montant_ticket >= 0),
  passager_trajets_max_par_mois       int CHECK (passager_trajets_max_par_mois >= 0),
  zone_sens_des_trajets               varchar(5) CHECK (zone_sens_des_trajets IN ('ET', 'OU', 'ET/OU')),
  zone_exclusion                      boolean NOT NULL DEFAULT false,
  zone_exclusion_liste                text,
  autre_exclusion                     text,
  trajet_longueur_min                 int CHECK (trajet_longueur_min >= 0),
  trajet_longueur_max                 int CHECK (trajet_longueur_max >= 0),
  trajet_classe_preuve                varchar(1)[] NOT NULL
    CHECK (cardinality(trajet_classe_preuve) > 0 AND trajet_classe_preuve <@ ARRAY['A', 'B', 'C']::varchar[]),
  lien_page_collectivite              varchar(500),
  autres_informations                 text,
  status                              varchar(10) NOT NULL DEFAULT 'draft' CHECK (status IN ('draft', 'validated')),
  validated_at                        timestamptz,
  validated_by                        int REFERENCES auth.users(_id) ON DELETE SET NULL,
  created_at                          timestamptz NOT NULL DEFAULT now(),
  updated_at                          timestamptz NOT NULL DEFAULT now(),
  deleted_at                          timestamptz,
  CHECK (created_by_operator_id IS NULL OR created_by_territory_id IS NULL)
);

CREATE UNIQUE INDEX campaign_conditions_territory_start_key
  ON policy.campaign_conditions (territory_type, territory_code, date_debut)
  WHERE deleted_at IS NULL;

CREATE INDEX campaign_conditions_operator_ids_idx ON policy.campaign_conditions USING gin (operator_ids);
