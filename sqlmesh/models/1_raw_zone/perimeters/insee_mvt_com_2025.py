import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from utils.loading import load_dataset
from utils.cleaning import clean_columns
from utils.url import get_last_url

# dictionnaire global des colonnes et types
COLUMN_TYPES = {
  "mod": "INTEGER",
  "date_eff": "VARCHAR",
  "typecom_av": "VARCHAR",
  "typecom_ap": "VARCHAR",
  "old_com": "VARCHAR",
  "new_com": "VARCHAR",
}

@model(
    "raw_zone.insee_mvt_com_2025",
    kind="FULL",
    cron="@yearly",
    columns=COLUMN_TYPES,
    tags=["raw","perimeters","insee_mvt_com_2025"],
)
def execute(
    context: ExecutionContext,
    start: datetime,
    end: datetime,
    execution_time: datetime,
    **kwargs: t.Any,
) -> pd.DataFrame:
    # --- Chargement des données ---
    df = load_dataset(
      path_or_bucket="geo-datasets-archives",
      key="insee_mvt_com_2025.csv",
      file_type="csv",
      column_types=COLUMN_TYPES,
      clean_col_name=True,
      rename_columns={
        "COM_AV": "old_com",
        "COM_AP": "new_com",
      },
    )
    return df
