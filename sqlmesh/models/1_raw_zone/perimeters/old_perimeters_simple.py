import typing as t
from datetime import datetime
import pandas as pd
from sqlmesh import ExecutionContext, model
from utils.loading import load_geo_dataset

# --- Définition des colonnes pour SQLMesh ---
GEOMETRY_COL = "geometry"
# --- Définition des colonnes pour SQLMesh ---
COLUMN_TYPES = {
    "year": "INTEGER",
    "arr": "VARCHAR",
    "l_arr": "VARCHAR",
    "com": "VARCHAR",
    "l_com": "VARCHAR",
    "epci": "VARCHAR",
    "l_epci": "VARCHAR",
    "aom": "VARCHAR",
    "l_aom": "VARCHAR",
    "dep": "VARCHAR",
    "l_dep": "VARCHAR",
    "reg": "VARCHAR",
    "l_reg": "VARCHAR",
    "country": "VARCHAR",
    "l_country": "VARCHAR",
    "pop": "INTEGER",
    'surface': 'FLOAT',
    "geometry": "TEXT"
}

@model(
    "raw_zone.old_perimeters_simple",
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
    key="old_perimeters.gpkg",
    layer="simple",
    column_types=COLUMN_TYPES,
    geometry_col=GEOMETRY_COL,
    target_crs="EPSG:4326"
  )

