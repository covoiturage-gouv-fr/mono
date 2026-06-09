import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from utils.loading import load_dataset
from utils.cleaning import clean_columns
from utils.url import get_last_url

# dictionnaire global des colonnes et types
COLUMN_TYPES = {
  "collectivite": "VARCHAR",
  "operateurs": "VARCHAR",
  "mise_a_jour": "DATE",
  "email": "VARCHAR",
  "type": "VARCHAR",
  "code": "VARCHAR",
  "premiere_campagne": "VARCHAR",
  "budget_incitations": "INTEGER",
  "date_debut": "DATE",
  "date_fin": "DATE",
  "conducteur_montant_max_par_passager": "FLOAT",
  "conducteur_montant_max_par_mois": "FLOAT",
  "conducteur_montant_min_par_passager": "FLOAT",
  "conducteur_trajets_max_par_mois": "INTEGER",
  "passager_trajets_max_par_mois": "INTEGER",
  "passager_gratuite": "VARCHAR",
  "passager_eligible_gratuite": "VARCHAR",
  "passager_reduction_ticket": "VARCHAR",
  "passager_eligibilite_reduction": "VARCHAR",
  "passager_montant_ticket": "VARCHAR",
  "zone_sens_des_trajets": "VARCHAR",
  "zone_exclusion": "VARCHAR",
  "si_zone_exclue_liste": "VARCHAR",
  "autre_exclusion": "VARCHAR",
  "trajet_longueur_min": "VARCHAR",
  "trajet_longueur_max": "VARCHAR",
  "trajet_classe_de_preuve": "VARCHAR",
  "autres_informations": "VARCHAR",
  "zone_sens_des_trajets_litteral": "VARCHAR",
  "lien_page_collectivite": "VARCHAR",
  "nom_plateforme": "VARCHAR",
}

@model(
    "raw_zone.campaigns",
    kind="FULL",
    cron="@weekly",
    columns=COLUMN_TYPES,
    tags=["raw", "campaigns"],
)
def execute(
    context: ExecutionContext,
    start: datetime,
    end: datetime,
    execution_time: datetime,
    **kwargs: t.Any,
) -> pd.DataFrame:
    # --- Chargement des données ---
    api_url = "https://www.data.gouv.fr/api/1/datasets/64a436118c609995b0386541"
    csv_url = get_last_url(api_url=api_url, path=["resources", "0", "latest"])
    df = load_dataset(
      path_or_bucket=csv_url, 
      file_type="csv",
      column_types=COLUMN_TYPES,
      clean_col_name=True,
      date_format="%d/%m/%Y"
    )
    return df
