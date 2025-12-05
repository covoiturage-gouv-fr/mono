import io
import os
import logging
import typing as t
from utils.s3 import get_s3_client
import pandas as pd
from dotenv import load_dotenv

def upload_to_s3(
    df: pd.DataFrame,
    key: str,
    bucket: t.Optional[str] = None,
    
) -> dict:
  log = logging.getLogger(__name__)
  extension = key.split(".")[-1].lower()
  if not os.getenv("S3_BUCKET") and not bucket:
    load_dotenv()
  getBucket = bucket or os.getenv("S3_BUCKET")
  if extension not in ("csv", "parquet"):
    raise ValueError(f"Format '{extension}' non supporté. Utilisez 'csv' ou 'parquet'.")
  try:
    s3_client = get_s3_client()
    buffer = io.BytesIO()
    if df.empty:
      raise ValueError(f"⚠️ DataFrame vide pour {key}")
    if extension == "csv":
      df.to_csv(buffer, index=False)
    else:  # parquet
      df.to_parquet(buffer, index=False)
    buffer.seek(0)
    file_size = buffer.getbuffer().nbytes  
    s3_client.put_object(
      Bucket=getBucket,
      Key=key,
      Body=buffer.getvalue(),
      ContentType="text/csv" if extension == "csv" else "application/octet-stream",
      Metadata={
        "rows": str(len(df)),
        "columns": str(len(df.columns)),
        "format": extension,
      },
    )
    result = {
      "status": "uploaded",
      "bucket": getBucket,
      "key": key,
      "format": extension,
      "size_bytes": file_size,
      "rows": len(df),
      "columns": len(df.columns),
      "s3_path": f"s3://{getBucket}/{key}",
      "date_uploaded": pd.Timestamp.now().isoformat(),
    }
    log.info(f"✅ {extension} uploadé : s3://{bucket}/{key}")
    return result
  except ValueError:
    raise
  except Exception as e:
    raise RuntimeError(f"❌ Erreur lors de l'upload : {str(e)}")
