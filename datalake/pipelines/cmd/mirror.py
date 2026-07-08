import os
from datetime import datetime, timezone
from typing import Optional

import typer
from dotenv import load_dotenv

from pipelines.helpers.config import load_config
from pipelines.helpers.s3 import s3_client, s3_exists, s3_path
from pipelines.helpers.url import download_url
from pipelines.helpers.checksum import hash_file

load_dotenv()
app = typer.Typer()


@app.command()
def mirror(
  config: str,
  bucket: Optional[str] = typer.Option(default=None, envvar="S3_BUCKET"),
  folder: Optional[str] = None,
):
  """Réconcilie les sources d'une config avec notre S3.

  - présente sur S3 → rien à faire ;
  - absente + `origin` accessible → recopiée sous une clé horodatée immuable, bloc config affiché ;
  - absente + sans `origin` → NOTICE : upload manuel requis (source instable derrière une interface
    bancale, ex. IGN — restera un geste humain).
  """
  tables = load_config(config)
  s3 = s3_client()
  seen: set[str] = set()
  mirrored, manual = 0, 0

  for t in tables:
    filename = t.get("filename", t["name"])
    ext = t.get("ext", "parquet")
    if filename in seen:
      continue
    seen.add(filename)

    key, _ = s3_path(filename, ext, bucket, folder)
    if s3_exists(bucket, key, s3):
      continue

    origin = t.get("origin")
    if not origin:
      manual += 1
      print(f"⚠️  {t['name']} : absent de S3, aucune `origin` → upload manuel requis (source instable, ex. IGN)")
      continue

    print(f"▶️  {t['name']} : absent → recopie depuis {origin}")
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    stem, _, extn = key.rpartition(".")
    tkey = f"{stem}.{ts}.{extn}"  # clé immuable horodatée
    path = download_url(origin, ext)
    size = os.path.getsize(path)
    digest = f"sha256:{hash_file(path, 'sha256')}"
    s3.upload_file(path, bucket, tkey)
    os.unlink(path)
    uploaded_at = s3.head_object(Bucket=bucket, Key=tkey)["LastModified"].isoformat()
    mirrored += 1
    print(f"   ✅ {tkey} — bloc config à committer :")
    print(f'      "filename": "{tkey.split("/")[-1].rsplit(".", 1)[0]}",')
    print(f'      "sha256": "{digest}",  "size": {size},  "uploaded_at": "{uploaded_at}"')

  print(f"\n{len(seen)} fichiers · {mirrored} recopié(s) · {manual} à uploader manuellement")


if __name__ == "__main__":
  app()
