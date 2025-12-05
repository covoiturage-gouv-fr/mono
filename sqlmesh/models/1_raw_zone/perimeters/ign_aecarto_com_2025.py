import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from utils.loading import load_geo_dataset

# --- Définition des colonnes pour SQLMesh ---
GEOMETRY_COL = "geometry"
COLUMN_TYPES = {
    "l_com": "VARCHAR",
    "com": "VARCHAR",
    "epci": "VARCHAR",
    "dep": "VARCHAR",
    "reg": "VARCHAR",
    "population": "INTEGER",
    GEOMETRY_COL: "TEXT"
}

@model(
    "raw_zone.ign_aecarto_com_2025",
    kind="FULL",
    columns=COLUMN_TYPES,
    tags=["raw", "perimeters", "ign_aecarto_com_2025"],
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
    key="ADE-COG-CARTO-PE_4-0_GPKG_WGS84G_FRA-ED2025-01-01.gpkg",
    layer="commune",
    column_types=COLUMN_TYPES,
    rename_columns={
      "nom_officiel": "l_com",
      "code_insee": "com",
      "code_insee_du_departement": "dep",
      "code_insee_de_la_region": "reg",
      "codes_siren_des_epci": "epci",
    },
    geometry_col=GEOMETRY_COL,
    target_crs="EPSG:4326"
  )

