import os
import io
import typing as t
import geopandas as gpd
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests
from utils.cleaning import auto_cast, clean_columns
from utils.s3 import get_s3_client


def load_geo_dataset(
    path_or_bucket: str,
    key: t.Optional[str] = None,
    layer: t.Optional[str] = None,
    column_types: dict[str, str] = {},
    rename_columns: t.Optional[dict[str, str]] = None,
    clean_col_name: bool = False,
    target_crs: str = "EPSG:4326",
    geometry_col: str = "geometry",
    s3_client: t.Optional[t.Any] = None,
    date_format: t.Optional[str] = None,
    date_default: t.Optional[str] = None,
) -> pd.DataFrame:
    """
    Charge un jeu de données géographique depuis un fichier local ou un bucket S3.
    Harmonise le CRS et cast les colonnes selon le mapping fourni.

    Args:
        path_or_bucket: Chemin local du fichier OU nom du bucket S3
        key: (Optionnel) Clé/chemin du fichier dans le bucket S3. Si non défini, on suppose un fichier local.
        layer: Nom de la couche à lire (pour les GPKG ou multi-layers)
        column_types: Mapping des colonnes pour SQLMesh
        rename_columns: dictionnaire {ancien_nom: nouveau_nom} pour renommer les colonnes
        clean_col_name: Si True, nettoie les noms de colonnes (minuscules, underscores, sans accents)
        target_crs: Code EPSG cible pour reprojection (par défaut "EPSG:4326")
        geometry_col: Nom de la colonne géométrique dans le GeoDataFrame
        s3_client: Client S3 existant (facultatif, sinon créé automatiquement)

    Returns:
        Un DataFrame prêt à être retourné dans un modèle SQLMesh
    """

    # --- Lecture du fichier ---
    if key:  # 🔹 Mode S3
      s3 = s3_client or get_s3_client()
      obj = s3.get_object(Bucket=path_or_bucket, Key=key)
      data_stream = io.BytesIO(obj["Body"].read())
      gdf = gpd.read_file(data_stream, layer=layer)
    else:  # 🔹 Mode local
      if not os.path.exists(path_or_bucket):
        raise FileNotFoundError(f"❌ Fichier introuvable : {path_or_bucket}")
      gdf = gpd.read_file(path_or_bucket, layer=layer)
    # --- Vérification de la colonne géométrique ---
    if geometry_col not in gdf.columns:
        raise ValueError(f"❌ La colonne géométrique '{geometry_col}' est introuvable dans les données.")
    # --- Harmonisation du CRS ---
    if gdf.crs is None:
      gdf.set_crs(target_crs, inplace=True)
    elif str(gdf.crs) != target_crs:
      gdf = gdf.to_crs(target_crs)
    # --- Conversion géométrie → WKT ---
    gdf_geom = gdf[geometry_col].apply(lambda x: x.wkt if x is not None else None)
    gdf_non_geom = gdf.drop(columns=[geometry_col])
    # --- Cast des colonnes non géométriques ---
    if rename_columns:
      gdf_non_geom = gdf_non_geom.rename(columns=rename_columns)
    if clean_col_name:
      gdf_non_geom = clean_columns(gdf_non_geom)
    gdf_non_geom = auto_cast(
      gdf_non_geom,
      {k: v for k, v in column_types.items() if k != geometry_col},
      date_format,
      date_default
    )
    # --- Recomposition du DataFrame final ---
    gdf_final = pd.concat([gdf_non_geom, gdf_geom.rename(geometry_col)], axis=1)

    return pd.DataFrame(gdf_final)   

def load_dataset(
    path_or_bucket: str,
    key: t.Optional[str] = None,
    rename_columns: t.Optional[dict[str, str]] = None,
    clean_col_name: bool = False,
    column_types: dict[str, str] = {},
    s3_client: t.Optional[t.Any] = None,
    file_type: t.Optional[str] = None,
    sheet_name: t.Optional[str] = None,
    sep: str = ",",
    encoding: str = "utf-8",
    date_format: t.Optional[str] = None,
    date_default: t.Optional[str] = None,
) -> pd.DataFrame:
    """
    Charge un jeu de données tabulaire (CSV, Excel, ODS, Parquet)
    depuis un fichier local ou un bucket S3, et cast les colonnes selon le mapping fourni.

    Args:
        path_or_bucket: Chemin local du fichier ou nom du bucket S3
        key: Clé/chemin du fichier dans le bucket (si S3)
        rename_columns: dictionnaire {ancien_nom: nouveau_nom} pour renommer les colonnes
        clean_col_name: Si True, nettoie les noms de colonnes (minuscules, underscores, sans accents)
        column_types: Mapping des colonnes et leurs types SQLMesh
        s3_client: Client S3 existant (facultatif)
        file_type: Type de fichier explicite ("csv", "excel", "ods", "parquet"). Si None, détecté depuis l’extension
        sheet_name: Nom ou index de la feuille (pour Excel/ODS)
        sep: Séparateur CSV (par défaut ",")
        encoding: Encodage du fichier (par défaut "utf-8")

    Returns:
        pd.DataFrame prêt à être retourné dans un modèle SQLMesh
    """
    # --- Déterminer la source (local / HTTP / S3) ---
    data: t.Union[str, io.BytesIO]
    if key:  # S3
        s3 = s3_client or get_s3_client()
        try:
            obj = s3.get_object(Bucket=path_or_bucket, Key=key)
        except Exception as e:
            raise FileNotFoundError(f"❌ S3 introuvable : s3://{path_or_bucket}/{key}") from e
        data = io.BytesIO(obj["Body"].read())
        file_path = key
    elif path_or_bucket.startswith(("http://", "https://")):
        try:
            resp = requests.get(path_or_bucket, stream=True, timeout=10)
            resp.raise_for_status()
            data = io.BytesIO(resp.content)
        except Exception as e:
            raise FileNotFoundError(f"❌ Fichier HTTP introuvable : {path_or_bucket}") from e
        file_path = path_or_bucket
    else:
        if not os.path.exists(path_or_bucket):
            raise FileNotFoundError(f"❌ Fichier local introuvable : {path_or_bucket}")
        data = path_or_bucket
        file_path = path_or_bucket

    # --- Détecter le type de fichier ---
    if file_type is None:
        ext = os.path.splitext(file_path)[1].lower()
        file_type = {
            ".csv": "csv", ".tsv": "csv",
            ".xlsx": "excel", ".xls": "excel",
            ".ods": "ods", ".parquet": "parquet",
        }.get(ext)
        if not file_type:
            raise ValueError(f"❌ Extension non reconnue : {file_path}")

    # --- Lecture unique selon le type ---
    read_func = {
        "csv": lambda: pd.read_csv(data, sep=sep, encoding=encoding, dtype=str),
        "excel": lambda: pd.read_excel(data, sheet_name=sheet_name, dtype=str),
        "ods": lambda: pd.read_excel(data, sheet_name=sheet_name, engine="odf", dtype=str),
        "parquet": lambda: pd.read_parquet(data),
    }.get(file_type)
    if not read_func:
        raise ValueError(f"❌ Type de fichier non supporté : {file_type}")
    df = read_func()
    # --- Post-traitement ---
    if rename_columns:
        df = df.rename(columns=rename_columns)
    if clean_col_name:
        df = clean_columns(df)
    df = auto_cast(df, column_types, date_format, date_default)
    df = df[list(column_types.keys())]
    return df

def load_parquet_chunked(
    path_or_bucket: str,
    key: t.Optional[str] = None,
    column_types: dict[str, str] = {},
    rename_columns: t.Optional[dict[str, str]] = None,
    clean_col_name: bool = False,
    chunk_size: int = 100_000,
    s3_client: t.Optional[t.Any] = None,
    date_format: t.Optional[str] = None,
    date_default: t.Optional[str] = None,
) -> t.Generator[pd.DataFrame, None, None]:
    """
    Charge un fichier Parquet volumineux par chunks pour économiser la mémoire.
    
    Args:
        path_or_bucket: Chemin local ou bucket S3
        key: Clé S3 (si applicable)
        column_types: Types des colonnes pour SQLMesh
        rename_columns: Renommage des colonnes
        clean_col_name: Nettoyer les noms de colonnes
        chunk_size: Nombre de lignes par chunk
        s3_client: Client S3 existant
        columns: Liste des colonnes à charger (None = toutes)
    
    Yields:
        DataFrames par chunks
    """
    # --- Déterminer la source ---
    if key:  # S3
        s3 = s3_client or get_s3_client()
        try:
            obj = s3.get_object(Bucket=path_or_bucket, Key=key)
            parquet_file = io.BytesIO(obj["Body"].read())
        except Exception as e:
            raise FileNotFoundError(f"❌ S3 introuvable : s3://{path_or_bucket}/{key}") from e
    else:  # Local
        if not os.path.exists(path_or_bucket):
            raise FileNotFoundError(f"❌ Fichier introuvable : {path_or_bucket}")
        parquet_file = path_or_bucket

    # --- Lecture par batches avec PyArrow ---
    parquet_table = pq.ParquetFile(parquet_file)    
    for batch in parquet_table.iter_batches(batch_size=chunk_size, columns=list(column_types.keys()), use_threads=True):
        
        # Conversion Arrow → Pandas
        df = batch.to_pandas()
        
        # --- Post-traitement ---
        if rename_columns:
            df = df.rename(columns=rename_columns)
        if clean_col_name:
            df = clean_columns(df)
        
        # Cast minimal (évite les conversions inutiles)
        df = auto_cast(df, column_types, date_format, date_default)
        yield df