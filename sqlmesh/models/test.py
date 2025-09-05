import typing as t
from datetime import datetime
import pandas as pd
import requests

from sqlmesh import ExecutionContext, model

def get_aires_last_url(api_url: str) -> str:
    """Récupère l'URL du dernier fichier CSV depuis l'API transport.data.gouv.fr"""
    resp = requests.get(api_url)
    resp.raise_for_status()
    data = resp.json()

    history = data.get("history", [])
    filtered = [h for h in history if h.get("payload", {}).get("schema_name") is not None]

    if not filtered:
        raise ValueError(f"Aucune entrée valide trouvée dans {api_url}")

    return filtered[0]["payload"]["permanent_url"]

@model(
    "test.aire_covoiturage",
    kind="FULL",
    columns={
        "id_lieu": "VARCHAR",
        "id_local": "VARCHAR",
        "nom_lieu": "VARCHAR",
        "ad_lieu": "VARCHAR",
        "com_lieu": "VARCHAR",
        "insee": "VARCHAR",
        "type": "VARCHAR",
        "date_maj": "DATE",
        "ouvert": "BOOLEAN",    
        "source": "VARCHAR",
        "long": "FLOAT",
        "lat": "FLOAT",
        "nbre_pl": "INTEGER",
        "nbre_pmr": "INTEGER",
        "duree": "VARCHAR",
        "horaires": "VARCHAR",
        "proprio": "VARCHAR",
        "lumiere": "VARCHAR",
        "comm": "VARCHAR",
        "dataset_id": "VARCHAR",
        "resource_id": "VARCHAR"
    },
    grain=("id_lieu", "date_maj"),
)



def execute(
    context: ExecutionContext,
    start: datetime,
    end: datetime,
    execution_time: datetime,
    **kwargs: t.Any,
) -> pd.DataFrame:
    api_url = "https://transport.data.gouv.fr/api/datasets/5d6eaffc8b4c417cdc452ac3"
    csv_url = get_aires_last_url(api_url)

    df = pd.read_csv(csv_url)

    # Renommage éventuel
    df = df.rename(columns={"Xlong": "long", "Ylat": "lat"})

    # Colonnes déclarées comme VARCHAR → cast en str
    varchar_cols = [
        "id_lieu", "id_local", "nom_lieu", "ad_lieu", "com_lieu",
        "insee", "type", "source", "duree", "horaires",
        "proprio", "lumiere", "comm", "dataset_id", "resource_id"
    ]
    for col in varchar_cols:
        if col in df.columns:
            df[col] = df[col].astype(str)

    return df
