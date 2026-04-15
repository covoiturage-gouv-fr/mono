import os
import boto3
from typing import Optional


def s3_config(
    endpoint: str | None = None,
    access_key: str | None = None,
    secret_key: str | None = None,
) -> dict:
    endpoint = endpoint or os.getenv("S3_ENDPOINT")
    access_key = access_key or os.getenv("S3_ACCESS_KEY")
    secret_key = secret_key or os.getenv("S3_SECRET_KEY")

    if not all([endpoint, access_key, secret_key]):
        raise RuntimeError("⚠️ Missing S3 credentials")

    if not endpoint.startswith("http"):
        endpoint = f"https://{endpoint}"

    return {"endpoint": endpoint, "access_key": access_key, "secret_key": secret_key}


def s3_client(
    endpoint: Optional[str] = None,
    access_key: Optional[str] = None,
    secret_key: Optional[str] = None,
):
    config = s3_config(endpoint, access_key, secret_key)
    return boto3.client(
        "s3",
        aws_access_key_id=config["access_key"],
        aws_secret_access_key=config["secret_key"],
        endpoint_url=config["endpoint"],
    )


def s3_exists(bucket: str, key: str, client=None) -> bool:
    """Vérifie si un objet existe dans le bucket S3."""
    client = client or s3_client()
    try:
        client.head_object(Bucket=bucket, Key=key)
        return True
    except client.exceptions.ClientError:
        return False


def s3_path(table: str, ext: str, bucket: str, folder: Optional[str] = None) -> tuple[str, str]:
    """Retourne (key, s3_uri) pour un fichier donné."""
    key = f"{folder}/{table}.{ext}" if folder else f"{table}.{ext}"
    path = f"s3://{bucket}/{key}"
    return key, path