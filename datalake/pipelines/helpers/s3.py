import os
import time
import boto3
import tempfile
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


def export_s3_client():
    return s3_client(
        endpoint=os.getenv("EXPORT_S3_ENDPOINT"),
        access_key=os.getenv("EXPORT_S3_ACCESS_KEY"),
        secret_key=os.getenv("EXPORT_S3_SECRET_KEY"),
    )


def s3_upload(bucket: str, key: str, local_path: str, client=None) -> None:
    client = client or export_s3_client()
    client.upload_file(local_path, bucket, key)


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

def s3_download(bucket: str, key: str, ext: str, client=None) -> str:
  client = client or s3_client()
  tmp = tempfile.NamedTemporaryFile(suffix=f".{ext}", delete=False)
  tmp.close()
  size = client.head_object(Bucket=bucket, Key=key)["ContentLength"]
  size_mo = size / 1e6
  print(f"  ↳ Téléchargement {key} ({size_mo:.0f} Mo)...")
  t0 = time.monotonic()
  client.download_file(bucket, key, tmp.name)
  elapsed = time.monotonic() - t0
  speed = f", {size_mo / elapsed:.0f} Mo/s" if elapsed else ""
  print(f"  ↳ Téléchargement terminé en {elapsed:.1f}s{speed}")
  return tmp.name