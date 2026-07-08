import json
import os
from typing import Optional

import typer
from dotenv import load_dotenv

from pipelines.helpers.config import load_config
from pipelines.helpers.checksum import hash_file
from pipelines.helpers.s3 import s3_client, s3_exists, s3_path, s3_download

load_dotenv()
app = typer.Typer()


@app.command()
def checksum(
  config: str,
  bucket: Optional[str] = typer.Option(default=None, envvar="S3_BUCKET"),
  folder: Optional[str] = None,
  write: bool = False,
):
  """Calcule le SHA256 de chaque fichier source S3 d'une config. `--write` l'inscrit en config.

  Les sources par URL (data.gouv, volatiles) sont ignorées : pas de checksum committé.
  """
  tables = load_config(config)
  s3 = s3_client()
  digests: dict[str, dict] = {}

  for t in tables:
    if "url" in t:
      continue
    filename = t.get("filename", t["name"])
    ext = t.get("ext", "parquet")
    if filename in digests:
      continue
    key, src = s3_path(filename, ext, bucket, folder)
    if not s3_exists(bucket, key, s3):
      print(f"❌ {filename} manquant : {src}")
      continue
    path = s3_download(bucket, key, ext, s3)
    digests[filename] = {"sha256": f"sha256:{hash_file(path, 'sha256')}", "size": os.path.getsize(path)}
    os.unlink(path)  # un seul fichier sur disque à la fois
    print(f"{filename}: {digests[filename]['sha256']}  ({digests[filename]['size']} octets)")

  if write:
    written = set()
    for t in tables:
      fn = t.get("filename", t["name"])
      if fn in digests and fn not in written:
        t["sha256"], t["size"] = digests[fn]["sha256"], digests[fn]["size"]
        written.add(fn)
    with open(config, "w") as f:
      json.dump(tables, f, indent=2, ensure_ascii=False)
      f.write("\n")
    print(f"✅ {len(written)} empreintes écrites dans {config}")


if __name__ == "__main__":
  app()
