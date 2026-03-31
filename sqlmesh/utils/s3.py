import os
import boto3
from dotenv import load_dotenv

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

def s3_file_exists(bucket: str, key: str, client=None) -> bool:
    """
    Vérifie si un fichier existe sur S3.
    """
    client = client or get_s3_client()
    try:
        client.head_object(Bucket=bucket, Key=key)
        return True
    except client.exceptions.ClientError:
        return False

def build_s3_path(bucket: str, folder: str, table: str, ext: str) -> tuple[str, str]:
    key = f"{folder}/{table}.{ext}"
    path = f"s3://{bucket}/{key}"
    return key, path