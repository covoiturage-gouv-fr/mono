import os
import boto3
from dotenv import load_dotenv
from typing import Optional

def s3_config(
  endpoint: str | None = None,
  access_key: str | None = None,
  secret_key: str | None = None,
):
  # Charger .env si nécessaire
  if not os.getenv("S3_ENDPOINT"):
    load_dotenv()
  endpoint = endpoint or os.getenv("S3_ENDPOINT")
  access_key = access_key or os.getenv("S3_ACCESS_KEY")
  secret_key = secret_key or os.getenv("S3_SECRET_KEY")

  if not all([endpoint, access_key, secret_key]):
    raise RuntimeError("⚠️ Missing S3 credentials")

  # Normalisation endpoint
  if not endpoint.startswith("http"):
    endpoint = f"https://{endpoint}"

  return {
    "endpoint": endpoint,
    "access_key": access_key,
    "secret_key": secret_key,
  }

def s3_client(
    endpoint: Optional[str] = None,
    access_key: Optional[str] = None,
    secret_key: Optional[str] = None,
):
    """
    Crée et retourne un client S3 configuré avec boto3.
    Args:
        endpoint: URL du service S3 (optionnel, sinon pris depuis .env)
        access_key: Clé d'accès S3 (optionnelle)
        secret_key: Clé secrète S3 (optionnelle)
    Returns:
        Un client boto3 configuré pour interagir avec le service S3.
    """
    config = s3_config(endpoint, access_key, secret_key)
    # Création du client boto3
    return boto3.client(
        "s3",
        aws_access_key_id=config["access_key"],
        aws_secret_access_key=config["secret_key"],
        endpoint_url=config["endpoint"],
    )

def s3_exists(bucket: str, key: str, client=None) -> bool:
    """
    Vérifie si un fichier existe sur S3.
    """
    client = client or s3_client()
    try:
        client.head_object(Bucket=bucket, Key=key)
        return True
    except client.exceptions.ClientError:
        return False

def s3_path(table: str, ext: str, bucket: str, folder: Optional[str] = None) -> tuple[str, str]:
  key = f"{folder}/{table}.{ext}" if folder else f"{table}.{ext}"
  path = f"s3://{bucket}/{key}"
  return key, path