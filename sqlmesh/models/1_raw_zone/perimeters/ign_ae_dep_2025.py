import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from utils.loading import load_geo_dataset

# --- Définition des colonnes pour SQLMesh ---
GEOMETRY_COL = "geometry"
COLUMN_TYPES = {
    "dep": "VARCHAR",
    "l_dep": "VARCHAR",
    GEOMETRY_COL: "TEXT"
}

@model(
    "raw_zone.ign_ae_dep_2025",
    kind="FULL",
    cron="@yearly",
    columns=COLUMN_TYPES,
    tags=["raw", "perimeters", "ign_ae_dep_2025"],
)
def execute(
  context: ExecutionContext,
  start: datetime,
  end: datetime,
  execution_time: datetime,
  **kwargs: t.Any,
) -> pd.DataFrame:
  return load_geo_dataset(
    path_or_bucket="geo-datasets-archives",
    key="ADE-COG_4-0_GPKG_WGS84G_FRA-ED2025-01-01.gpkg",
    layer="departement",
    column_types=COLUMN_TYPES,
    rename_columns={
      "nom_officiel": "l_dep",
      "code_insee": "dep",
    },
    geometry_col=GEOMETRY_COL,
    target_crs="EPSG:4326"
  )

