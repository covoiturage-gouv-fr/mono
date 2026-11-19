"""Client OSRM : plus court chemin entre deux points, avec la suite des nœuds OSM traversés."""

from dataclasses import dataclass

from pipelines.helpers.retry import retry

ROUTE_PARAMS = {
  "overview": "full",
  "geometries": "geojson",
  "annotations": "nodes",
  "steps": "false",
}
# OSRM répond 400 quand il n'y a pas de chemin : c'est un résultat, pas une panne.
NO_ROUTE_CODES = {"NoRoute", "NoSegment"}


@dataclass(frozen=True)
class Pair:
  lng_1: float
  lat_1: float
  lng_2: float
  lat_2: float


@dataclass(frozen=True)
class Route:
  nodes: list[int]
  coords: list[tuple[float, float]]
  distance: float
  duration: float


class TransientOsrmError(Exception):
  pass


def route_url(base_url: str, pair: Pair) -> str:
  return (
    f"{base_url.rstrip('/')}/route/v1/driving/"
    f"{pair.lng_1},{pair.lat_1};{pair.lng_2},{pair.lat_2}"
  )


def parse_route(payload: dict) -> Route | None:
  code = payload.get("code")
  if code in NO_ROUTE_CODES:
    return None
  if code != "Ok":
    raise ValueError(f"réponse OSRM inattendue : {code} {payload.get('message', '')}")
  route = payload["routes"][0]
  nodes = route["legs"][0]["annotation"]["nodes"]
  coords = [tuple(c) for c in route["geometry"]["coordinates"]]
  # Avec overview=full sur une seule étape, la géométrie a un point par nœud :
  # c'est ce qui permet de se passer d'un extrait OSM pour placer les nœuds.
  if len(nodes) != len(coords):
    raise ValueError(f"{len(nodes)} nodes pour {len(coords)} coordonnées")
  return Route(nodes=nodes, coords=coords, distance=route["distance"], duration=route["duration"])


def fetch_route(session, base_url: str, pair: Pair, attempts: int = 4, timeout: float = 30) -> Route | None:
  url = route_url(base_url, pair)

  def call():
    response = session.get(url, params=ROUTE_PARAMS, timeout=timeout)
    if response.status_code == 429 or response.status_code >= 500:
      raise TransientOsrmError(f"HTTP {response.status_code}")
    return parse_route(response.json())

  return retry(call, attempts=attempts, label="route OSRM")
