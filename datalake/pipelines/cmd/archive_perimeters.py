import json
import os
from datetime import datetime, timezone
from typing import Optional

import typer
from dotenv import load_dotenv

from pipelines.helpers.s3 import s3_client, s3_upload
from pipelines.tasks.db_sync import export_geo

load_dotenv()
app = typer.Typer()

# Couplés à `models/trusted/perimeters.sql` (colonnes `old_perimeters`) — à faire évoluer ensemble.
_LAYERS = {
  "simple": {
    "seed_name": "old_perimeters_simple",
    "select": [
      "year", "arr", "l_arr", "com", "l_com", "epci", "l_epci", "aom", "l_aom",
      "dep", "l_dep", "reg", "l_reg", "country", "l_country", "pop", "surface",
      "geom_simple AS geom",
    ],
  },
  "full": {
    "seed_name": "old_perimeters_full",
    "select": ["year", "arr", "l_arr", "geom"],
  },
  "centroid": {
    "seed_name": "old_perimeters_centroid",
    "select": ["year", "arr", "l_arr", "centroid AS geom"],
  },
}


@app.command()
def archive(
  table: str = "perimeters",
  schema: str = "zone_trusted",
  bucket: Optional[str] = typer.Option(default=None, envvar="S3_BUCKET"),
  folder: str = "seeds",
  seed_config: str = "pipelines/config/raw/seed_raw.json",
):
  """Régénère le GPKG 3-layers `old_perimeters` depuis `{schema}.{table}` et l'archive sur S3.

  Sens inverse de `seed` (ogr2ogr Postgres → GPKG, un run par layer). Met à jour `filename` pour
  les 3 entrées `old_perimeters_*` dans `seed_config` ; ne calcule PAS le sha256/size — lancer
  ensuite `just checksum raw/seed_raw.json --folder seeds --write` (télécharge et vérifie le
  fichier fraîchement uploadé, committe l'empreinte). Le bucket range tout sous `seeds/` : sans
  `--folder seeds`, checksum cherche à la racine et échoue avec « manquant ».
  """
  ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
  stem = f"old_perimeters.{ts}"
  local_path = f"{stem}.gpkg"

  print(f"▶️  Génération de {local_path} depuis {schema}.{table}")
  for i, (layer, spec) in enumerate(_LAYERS.items()):
    export_geo(table=table, schema=schema, layer=layer, select=spec["select"], path=local_path, update=i > 0)
    print(f"  ✅ layer {layer}")

  key = f"{folder}/{local_path}" if folder else local_path
  s3 = s3_client()
  print(f"▶️  Upload s3://{bucket}/{key}")
  s3_upload(bucket, key, local_path, client=s3)
  size = os.path.getsize(local_path)
  os.unlink(local_path)
  print(f"✅ Uploadé — {size:_} octets".replace("_", " "))

  names = {spec["seed_name"] for spec in _LAYERS.values()}
  with open(seed_config) as f:
    tables = json.load(f)
  updated = 0
  for t in tables:
    if t["name"] in names:
      t["filename"] = stem
      updated += 1
  with open(seed_config, "w") as f:
    json.dump(tables, f, indent=2, ensure_ascii=False)
    f.write("\n")

  print(f"✅ {updated} entrées mises à jour dans {seed_config} (filename → {stem})")
  print("👉  Reste à lancer : just checksum raw/seed_raw.json --folder seeds --write")


if __name__ == "__main__":
  app()
