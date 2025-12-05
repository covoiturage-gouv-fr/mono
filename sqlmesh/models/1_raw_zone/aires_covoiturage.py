import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from utils.url import get_last_url
from utils.loading import load_dataset

COLUMN_TYPES = {
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
}

@model(
    "raw_zone.aires_covoiturage",
    kind="FULL",
    columns=COLUMN_TYPES,
    grain=("id_lieu", "date_maj"),
    tags=["raw", "aires_covoiturage"],
)

def execute(
    context: ExecutionContext,
    start: datetime,
    end: datetime,
    execution_time: datetime,
    **kwargs: t.Any,
) -> pd.DataFrame:
    api_url = "https://transport.data.gouv.fr/api/datasets/5d6eaffc8b4c417cdc452ac3"
    csv_url = get_last_url(api_url=api_url, path=["history", "0", "payload", "resource_url"])
    df = load_dataset(
      path_or_bucket=csv_url, 
      file_type="csv",
      column_types=COLUMN_TYPES,
      rename_columns={
          "Xlong": "long",
          "Ylat": "lat"
      },
    )
    return df
