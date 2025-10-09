import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from utils.loading import load_geo_dataset

# --- Définition des colonnes pour SQLMesh ---
GEOMETRY_COL = "geometry"
COLUMN_TYPES = {
    "reg": "VARCHAR",
    "l_reg": "VARCHAR",
    GEOMETRY_COL: "TEXT"
}

@model(
    "raw_zone.ign_ae_reg_2025",
    kind="FULL",
    columns=COLUMN_TYPES,
    post_statements=[f"ALTER TABLE @this_model ALTER COLUMN {GEOMETRY_COL} TYPE geometry USING ST_SetSRID(ST_GeomFromText({GEOMETRY_COL}, 4326), 4326);"],
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
    layer="region",
    column_types=COLUMN_TYPES,
    rename_columns={
      "nom_officiel": "l_reg",
      "code_insee": "reg",
    },
    geometry_col=GEOMETRY_COL,
    target_crs="EPSG:4326"
  )

