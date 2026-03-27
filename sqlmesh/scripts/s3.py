import os
import boto3
from dotenv import load_dotenv
from typing import Optional

def get_s3_client(
    endpoint: str | None = None,
    access_key: str | None = None,
    secret_key: str | None = None,
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
    # Charger .env une seule fois si nécessaire
    if not os.getenv("S3_ENDPOINT"):
        load_dotenv()

    # Récupérer les credentials
    endpoint = endpoint or os.getenv("S3_ENDPOINT")
    if endpoint and not endpoint.startswith("http"):
      endpoint = f"https://{endpoint}"
    access_key = access_key or os.getenv("S3_ACCESS_KEY")
    secret_key = secret_key or os.getenv("S3_SECRET_KEY")

    # Vérification
    if not all([endpoint, access_key, secret_key]):
        raise RuntimeError("⚠️ Missing S3 credentials (endpoint, access_key, or secret_key)")

    # Création du client boto3
    return boto3.client(
        "s3",
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        endpoint_url=endpoint,
    )

def s3_file_exists(key: str, bucket: Optional[str] = None) -> bool:
  if not os.getenv("S3_BUCKET") and not bucket:
    load_dotenv()
  getBucket = bucket or os.getenv("S3_BUCKET")
  try:
    s3_client = get_s3_client()
    s3_client.head_object(Bucket=getBucket, Key=key)
    return True
  except Exception as e:
    if hasattr(e, "response") and e.response.get("Error", {}).get("Code") in ("404", "NoSuchKey"):
      return False
    raise
