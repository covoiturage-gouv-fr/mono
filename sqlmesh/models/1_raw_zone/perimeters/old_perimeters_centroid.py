
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
    GEOMETRY_COL: "TEXT"
}

@model(
    "raw_zone.old_perimeters_centroid",
    kind="FULL",
    columns=COLUMN_TYPES,
    tags=["raw", "perimeters", "old_perimeters_centroid"],
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
    layer="centroid",
    column_types=COLUMN_TYPES,
    geometry_col=GEOMETRY_COL,
    target_crs="EPSG:4326"
  )

