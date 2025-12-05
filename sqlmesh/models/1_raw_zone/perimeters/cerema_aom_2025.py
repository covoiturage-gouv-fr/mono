import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from utils.loading import load_dataset

# dictionnaire global des colonnes et types
COLUMN_TYPES = {
  "com": "VARCHAR",
  "aom": "VARCHAR",
  "l_aom": "VARCHAR",
}

@model(
    "raw_zone.cerema_aom_2025",
    kind="FULL",
    columns=COLUMN_TYPES,
    tags=["raw","perimeters","cerema_aom_2025"],
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
      key="cerema_aom_2025.csv",
      file_type="csv",
      column_types=COLUMN_TYPES,
      rename_columns={
        "code_insee": "com",
      },
    )
    return df
