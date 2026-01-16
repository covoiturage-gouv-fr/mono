import os
import typing as t
from utils.s3 import get_s3_client
import pandas as pd
from dotenv import load_dotenv
from boto3.s3.transfer import TransferConfig

def upload_to_s3(
    file_path: str,
    key: str,
    bucket: t.Optional[str] = None,
) -> dict:
  extension = key.split(".")[-1].lower()
  if not os.getenv("S3_BUCKET") and not bucket:
    load_dotenv()
  getBucket = bucket or os.getenv("S3_BUCKET")
  if extension not in ("csv", "parquet"):
    raise ValueError(f"Format '{extension}' non supporté. Utilisez 'csv' ou 'parquet'.")
  try:
    s3_client = get_s3_client()
    file_size = os.path.getsize(file_path)
    print(f"▶️ Upload S3 : {file_path} → s3://{getBucket}/{key}")
    config = TransferConfig(
      multipart_threshold=1024 * 25,  # 25 MB
      max_concurrency=10,
      multipart_chunksize=1024 * 25,
      use_threads=True
    )
  
    s3_client.upload_file(
      file_path,
      getBucket,
      key,
      Config=config,
      ExtraArgs={
        "ContentType": "text/csv"
        if extension == "csv"
        else "application/octet-stream"
      },
    )
    print(f"✅ Upload terminé : s3://{getBucket}/{key}")
    os.remove(file_path)
    return {
        "status": "uploaded",
        "bucket": getBucket,
        "key": key,
        "format": extension,
        "size_bytes": file_size,
        "s3_path": f"s3://{getBucket}/{key}",
        "date_uploaded": pd.Timestamp.now().isoformat(),
    }
  except ValueError:
    raise
  except Exception as e:
    raise RuntimeError(f"❌ Erreur lors de l'upload : {str(e)}")
