import typing as t
from datetime import datetime
import pandas as pd
import requests
from sqlmesh import ExecutionContext, model
from utils.loading import load_dataset
from utils.cleaning import clean_columns

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

def get_last_url(api_url: str) -> str:
    resp = requests.get(api_url)
    resp.raise_for_status()
    data = resp.json()
    resources = data.get("resources", [])
    if not resources:
        raise ValueError(f"Aucune entrée valide trouvée dans {api_url}")
    return resources[0]["latest"]

@model(
    "raw_zone.incentive_campaigns",
    kind="FULL",
    columns=COLUMN_TYPES,
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
    csv_url = get_last_url(api_url)
    df = load_dataset(csv_url)
    # --- Nettoyage et cast ---
    df = clean_columns(df)
    return df
