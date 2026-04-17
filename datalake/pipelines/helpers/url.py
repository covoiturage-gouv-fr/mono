import typing as t
import requests

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