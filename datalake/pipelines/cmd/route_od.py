"""Route les paires de cellules H3 des trajets covoiturés et met les routes en cache.

Lit les paires orientées de cellules H3 z8 de la fenêtre de routage (modèle dbt
`h3_od_month`, à construire avant), route celles absentes de `zone_raw.osrm_route`
entre les centres des cellules et écrit les résultats par lots. Interruptible :
un lot écrit n'est jamais reroute.

  just dbt run --select h3_od_month
  just route-od --limit 1000
"""

import os
import time
from concurrent.futures import ThreadPoolExecutor
from itertools import islice
from typing import Iterable, Optional

import requests
import typer
from dotenv import load_dotenv

from pipelines.helpers.osrm import Pair, Route, fetch_route
from pipelines.helpers.pg import pg_connect

load_dotenv()
app = typer.Typer()

# Créées ici et non par dbt : un --full-refresh viderait des heures de routage.
# La clé porte des coordonnées (centres de cellules H3) : une paire déjà routée un
# mois précédent réutilise sa route.
# `dataset` = date de l'import OSM servi par OSRM ; un nouvel import se route à côté.
TABLES_SQL = """
  CREATE SCHEMA IF NOT EXISTS zone_raw;

  CREATE TABLE IF NOT EXISTS zone_raw.osrm_route (
    dataset   text          NOT NULL,
    lng_1     numeric(9, 6) NOT NULL,
    lat_1     numeric(9, 6) NOT NULL,
    lng_2     numeric(9, 6) NOT NULL,
    lat_2     numeric(9, 6) NOT NULL,
    status    text          NOT NULL CHECK (status IN ('ok', 'no_route')),
    distance  real,
    duration  real,
    nodes     bigint[],
    routed_at timestamptz   NOT NULL DEFAULT now(),
    PRIMARY KEY (dataset, lng_1, lat_1, lng_2, lat_2)
  );

  CREATE TABLE IF NOT EXISTS zone_raw.osrm_node (
    dataset text          NOT NULL,
    node_id bigint        NOT NULL,
    lng     numeric(9, 6) NOT NULL,
    lat     numeric(9, 6) NOT NULL,
    PRIMARY KEY (dataset, node_id)
  );
"""

# Paires orientées départ → arrivée, avec les centres de leurs cellules :
# construites par le modèle dbt h3_od_month, à lancer avant.
PAIRS_SQL = """
  SELECT DISTINCT lng_1, lat_1, lng_2, lat_2
  FROM zone_aggregated.h3_od_month
"""


def ensure_tables(conn) -> None:
  conn.execute(TABLES_SQL)


def batched(items: Iterable, size: int):
  it = iter(items)
  while chunk := list(islice(it, size)):
    yield chunk


def missing_pairs(conn, dataset: str, limit: Optional[int] = None) -> list[Pair]:
  sql = f"""
    WITH od AS ({PAIRS_SQL})
    SELECT od.lng_1, od.lat_1, od.lng_2, od.lat_2
    FROM od
    WHERE NOT EXISTS (
      SELECT 1 FROM zone_raw.osrm_route AS r
      WHERE
        r.dataset = %(dataset)s
        AND r.lng_1 = od.lng_1 AND r.lat_1 = od.lat_1
        AND r.lng_2 = od.lng_2 AND r.lat_2 = od.lat_2
    )
    ORDER BY 1, 2, 3, 4
    {"LIMIT %(limit)s" if limit else ""}
  """
  rows = conn.execute(sql, {"dataset": dataset, "limit": limit}).fetchall()
  return [Pair(*(float(v) for v in row)) for row in rows]


def route_row(dataset: str, pair: Pair, route: Optional[Route]) -> tuple:
  if route is None:
    return (dataset, pair.lng_1, pair.lat_1, pair.lng_2, pair.lat_2, "no_route", None, None, None)
  return (
    dataset, pair.lng_1, pair.lat_1, pair.lng_2, pair.lat_2,
    "ok", route.distance, route.duration, route.nodes,
  )


def node_rows(dataset: str, routes: Iterable[Optional[Route]]) -> list[tuple]:
  coords = {}
  for route in routes:
    if route is not None:
      coords.update(zip(route.nodes, route.coords))
  return [(dataset, node, lng, lat) for node, (lng, lat) in coords.items()]


def write_batch(conn, dataset: str, results: list[tuple[Pair, Optional[Route]]]) -> None:
  with conn.transaction():
    conn.execute(
      "CREATE TEMP TABLE IF NOT EXISTS tmp_osrm_route (LIKE zone_raw.osrm_route INCLUDING DEFAULTS) ON COMMIT DELETE ROWS"
    )
    conn.execute(
      "CREATE TEMP TABLE IF NOT EXISTS tmp_osrm_node (LIKE zone_raw.osrm_node) ON COMMIT DELETE ROWS"
    )
    cur = conn.cursor()
    with cur.copy(
      "COPY tmp_osrm_route (dataset, lng_1, lat_1, lng_2, lat_2, status, distance, duration, nodes) FROM STDIN"
    ) as copy:
      for pair, route in results:
        copy.write_row(route_row(dataset, pair, route))
    with cur.copy("COPY tmp_osrm_node (dataset, node_id, lng, lat) FROM STDIN") as copy:
      for row in node_rows(dataset, (route for _, route in results)):
        copy.write_row(row)
    conn.execute("INSERT INTO zone_raw.osrm_route SELECT * FROM tmp_osrm_route ON CONFLICT DO NOTHING")
    conn.execute("INSERT INTO zone_raw.osrm_node SELECT * FROM tmp_osrm_node ON CONFLICT DO NOTHING")


@app.command()
def run(
  dataset: str = typer.Option(os.getenv("OSRM_DATASET", "2023-12-21"), help="Import OSM servi par OSRM"),
  osrm_url: str = typer.Option(os.getenv("OSRM_URL", "http://localhost:5000")),
  concurrency: int = typer.Option(2, help="Requêtes simultanées ; l'instance sert aussi l'API de prod"),
  batch_size: int = typer.Option(500),
  limit: Optional[int] = typer.Option(None, help="Nombre maximal de paires à router"),
  dry_run: bool = typer.Option(False, help="Compte les paires à router sans appeler OSRM"),
):
  conn = pg_connect()
  ensure_tables(conn)
  pairs = missing_pairs(conn, dataset, limit)
  print(f"🧭 {len(pairs)} paires à router (dataset {dataset}) via {osrm_url}")
  if dry_run or not pairs:
    return

  session = requests.Session()
  session.mount("http://", requests.adapters.HTTPAdapter(pool_maxsize=concurrency))
  done, no_route, start = 0, 0, time.perf_counter()
  with ThreadPoolExecutor(concurrency) as pool:
    for chunk in batched(pairs, batch_size):
      routes = list(pool.map(lambda p: fetch_route(session, osrm_url, p), chunk))
      write_batch(conn, dataset, list(zip(chunk, routes)))
      done += len(chunk)
      no_route += sum(r is None for r in routes)
      rate = done / (time.perf_counter() - start)
      print(f"  {done}/{len(pairs)} ({rate:.1f} routes/s, {no_route} sans route)")
  print(f"🏁 {done} paires routées, {no_route} sans route")


if __name__ == "__main__":
  app()
