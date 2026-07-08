import tempfile
import typing as t
from urllib.parse import urlparse

import requests

from pipelines.helpers.retry import retry

_MAX_DOWNLOAD_BYTES = 5 * (1 << 30)  # plafond par défaut : 5 Gio (couvre les GPKG IGN)


def download_url(url: str, ext: str, max_bytes: int = _MAX_DOWNLOAD_BYTES) -> str:
  """Télécharge une URL vers un fichier local temporaire (le loader lit un fichier, pas un flux).

  https uniquement ; plafonne la taille (annoncée + réelle) pour ne pas saturer le disque.
  """
  if urlparse(url).scheme != "https":
    raise ValueError(f"❌ URL non https refusée : {url}")

  tmp = tempfile.NamedTemporaryFile(suffix=f".{ext}", delete=False)
  tmp.close()

  def _fetch():
    with requests.get(url, stream=True, timeout=60) as r:
      r.raise_for_status()
      declared = r.headers.get("Content-Length")
      if declared and int(declared) > max_bytes:
        raise RuntimeError(f"❌ taille annoncée ({declared} o) au-delà du plafond {max_bytes} o : {url}")
      written = 0
      with open(tmp.name, "wb") as f:
        for chunk in r.iter_content(1 << 16):
          written += len(chunk)
          if written > max_bytes:
            raise RuntimeError(f"❌ taille au-delà du plafond {max_bytes} o : {url}")
          f.write(chunk)

  retry(_fetch, label=f"téléchargement {url}")
  return tmp.name

def find_in_json(data: t.Any, path: list[str]) -> t.Any:
  """
  Parcourt récursivement un objet JSON (dict/list) selon une liste de clés et/ou d’index.

  Args:
      data: Objet JSON (dict, list, etc.)
      path: Liste des clés / index à suivre, ex: ["resources", "0", "latest"]

  Returns:
      La valeur trouvée au bout du chemin.
  """
  obj = data
  for k in path:
    if isinstance(obj, list):
      try:
        obj = obj[int(k)]
      except (ValueError, IndexError):
        raise ValueError(f"❌ Index invalide '{k}' dans la liste")
    elif isinstance(obj, dict):
      if k not in obj:
        raise KeyError(f"❌ Clé '{k}' absente dans l'objet JSON")
      obj = obj[k]
    else:
      raise TypeError(f"❌ Impossible de descendre dans un objet de type {type(obj)} à '{k}'")
  return obj

def get_last_url(api_url: str, path: list[str]) -> str:
  """
  Récupère une URL depuis une API JSON selon une arborescence donnée.
  Args:
      api_url: URL de l'API à interroger
      path: liste des clés/index à suivre dans le JSON,
            ex: ["resources", "0", "latest"] ou ["history", "0", "payload", "permanent_url"]

  Returns:
      L'URL trouvée selon le chemin spécifié.
  """
  resp = requests.get(api_url)
  resp.raise_for_status()
  data = resp.json()
  try:
    return find_in_json(data, path)
  except Exception as e:
    raise ValueError(f"❌ Impossible de trouver l'URL via le chemin {path}: {e}")