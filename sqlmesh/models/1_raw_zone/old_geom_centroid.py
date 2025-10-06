import os
import typing as t
from datetime import datetime
import pandas as pd
import geopandas as gpd
import boto3
import io
from sqlmesh import ExecutionContext, model
from utils.cleaning import auto_cast
from dotenv import load_dotenv

# Charger les variables d'environnement à la racine du projet
load_dotenv()

# --- Définition des colonnes pour SQLMesh ---
COLUMN_TYPES = {
    "id": "VARCHAR",
    "year": "INTEGER",
    "arr": "VARCHAR",
    "l_arr": "VARCHAR",
    "geometry": "TEXT"
}

@model(
    "raw_zone.old_geom_centroid",
    kind="FULL",
    columns=COLUMN_TYPES,
    post_statements=["ALTER TABLE @this_model ALTER COLUMN geometry TYPE geometry USING ST_SetSRID(ST_GeomFromText(geometry, 4326), 4326);"],
)
def execute(
    context: ExecutionContext,
    start: datetime,
    end: datetime,
    execution_time: datetime,
    **kwargs: t.Any,
) -> pd.DataFrame:

    # --- Récupérer les credentials depuis .env ---
    access_key = os.getenv("S3_ACCESS_KEY")
    secret_key = os.getenv("S3_SECRET_KEY")
    endpoint = os.getenv("S3_ENDPOINT")

    if not access_key or not secret_key or not endpoint:
        raise RuntimeError("⚠️ S3 credentials not found in environment variables")

    # --- Connexion S3 ---
    s3 = boto3.client(
        "s3",
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        endpoint_url=endpoint,
    )

    # --- Lecture du GPKG depuis S3 ---
    obj = s3.get_object(
        Bucket="geo-datasets-archives",
        Key="perimeters_centroid.gpkg"
    )
    gdf = gpd.read_file(io.BytesIO(obj['Body'].read()))

    # --- Harmonisation du CRS ---
    if gdf.crs is None:
      gdf.set_crs("EPSG:4326", inplace=True)
    else:
      gdf = gdf.to_crs("EPSG:4326")
    # --- Sélection des colonnes à conserver ---
    gdf_geom = gdf["geometry"].apply(lambda x: x.wkt if x is not None else None)  # conserver la géométrie en WKT
    gdf_non_geom = gdf.drop(columns=['geometry'])  # DataFrame sans la géométrie
    gdf_non_geom = auto_cast(gdf_non_geom, {k: v for k, v in COLUMN_TYPES.items() if k != 'geometry'})
    # --- Cast uniquement les colonnes non géométriques ---
    gdf_final = pd.concat([gdf_non_geom, gdf_geom], axis=1)
    # --- Reconstruction du GeoDataFrame sans toucher à la géométrie ---
    return pd.DataFrame(gdf_final)

