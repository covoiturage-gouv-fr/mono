import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from utils.loading import load_geo_dataset

# --- Définition des colonnes pour SQLMesh ---
GEOMETRY_COL = "geometry"
COLUMN_TYPES = {
    "nom_officiel": "VARCHAR",
    "code_insee": "VARCHAR",
    "population": "INTEGER",
    "code_insee_du_departement": "VARCHAR",
    "code_insee_de_la_region": "VARCHAR",
    "codes_siren_des_epci": "VARCHAR",
    GEOMETRY_COL: "TEXT"
}

@model(
    "raw_zone.ign_adminexpress_2025",
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
    layer="commune",
    column_types=COLUMN_TYPES,
    geometry_col=GEOMETRY_COL,
    target_crs="EPSG:4326"
  )

