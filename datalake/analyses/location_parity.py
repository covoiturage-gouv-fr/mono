#!/usr/bin/env python3
"""Harnais de parité heatmap `location` : ancienne requête à la volée vs pré-agrégat.

Compare, hexagone par hexagone, la sortie de l'ANCIENNE requête
`build_location_query` (scan de `zone_exposed.location` + `observatory_perimeters`)
à la NOUVELLE lecture du pré-agrégat `zone_exposed.location_<grain>` (PR #3293).

À lancer APRÈS `dbt build` des modèles `location_*` sur un backfill scopé, avant
de faire confiance à la publication. Les deux côtés lisent la même base : tout
écart résiduel = bug de logique d'agrégation (et non une différence de source).

    DBT_HOST=... DBT_PORT=... DBT_USER=... DBT_PASSWORD=... DBT_DBNAME=datalake \
        python analyses/location_parity.py

Sortie : une ligne par scope (PASS / DIVERGE attendu / FAIL), code retour != 0
si un seul FAIL. La matrice couvre chaque branche de la sémantique (intra/inter,
NULL/cross-border, PLM, résolution de périmètre au millésime, aom vs aomreg).
"""

from __future__ import annotations

import sys
from dataclasses import dataclass

from pipelines.helpers.pg import pg_connect

# Binning de comparaison : un zoom « moyen ». La parité doit tenir à tout n<=8 ;
# n=6 suffit à détecter tout écart de comptage (le binning est déterministe).
N_ZOOM = 6

# com -> arr : même mapping que l'ancienne API (SELECT arr AS com ...).
_PERIM_COL = {"com": "arr", "epci": "epci", "aom": "aom",
              "dep": "dep", "reg": "reg", "country": "country"}

# Communes PLM : l'ancienne requête (WHERE arr = code) renvoyait un heatmap VIDE ;
# le nouveau pré-agrégat renvoie des données (fix voulu). Écart ATTENDU, pas un échec.
PLM_COM_CODES = {"75056", "69123", "13055"}


@dataclass(frozen=True)
class Scope:
    type_: str
    code: str
    grain: str            # month | quarter | semester | year
    year: int
    period: int | None    # mois 1-12 / trimestre 1-4 / semestre 1-2 / None pour year
    note: str = ""


# Matrice : chaque ligne exerce une branche distincte. Codes réels (INSEE COG) ;
# `XXXXX` = code pays France. Vérifier que chaque scope a du volume dans sa période
# (un territoire vide rend le test vacant).
SCOPES: list[Scope] = [
    Scope("com", "31555", "month", 2024, 6, "commune ordinaire (intra + inter)"),
    Scope("com", "75056", "year", 2024, None, "commune PLM -> DIVERGENCE attendue"),
    Scope("reg", "76", "month", 2024, 6, "région (résolution périmètre au millésime)"),
    Scope("reg", "11", "year", 2024, None, "région dense, grain année"),
    Scope("dep", "31", "quarter", 2024, 2, "département, grain trimestre"),
    Scope("aom", "217500016", "semester", 2024, 1, "aom réelle, grain semestre"),
    Scope("country", "XXXXX", "year", 2024, None, "pays entier, pire cas dense"),
]


def _period_bounds(s: Scope) -> tuple[str, str]:
    """Bornes [début, fin) ISO pour le filtre temporel de l'ancienne requête."""
    y = s.year
    if s.grain == "month":
        m = s.period
        end = (f"{y + 1}-01-01" if m == 12 else f"{y}-{m + 1:02d}-01")
        return f"{y}-{m:02d}-01", end
    if s.grain == "quarter":
        sm = (s.period - 1) * 3 + 1
        end = (f"{y + 1}-01-01" if sm + 3 > 12 else f"{y}-{sm + 3:02d}-01")
        return f"{y}-{sm:02d}-01", end
    if s.grain == "semester":
        sm = 1 if s.period == 1 else 7
        end = (f"{y + 1}-01-01" if sm == 7 else f"{y}-07-01")
        return f"{y}-{sm:02d}-01", end
    return f"{y}-01-01", f"{y + 1}-01-01"


def _old_new_sql(s: Scope, n: int) -> tuple[str, dict]:
    """Une seule requête : ancienne sémantique (CTE `old`) vs lecture du pré-agrégat
    (CTE `new`), jointure externe par hex, renvoie les stats d'écart + 5 échantillons."""
    col = _PERIM_COL[s.type_]
    dt_start, dt_end = _period_bounds(s)
    params: dict = {"code": s.code, "year": s.year, "n": n,
                    "dt_start": dt_start, "dt_end": dt_end}

    # Filtre de grain côté nouvelle table exposée.
    grain_pred = ""
    if s.grain != "year":
        gcol = {"month": "month", "quarter": "quarter", "semester": "semester"}[s.grain]
        grain_pred = f"AND {gcol} = %(period)s"
        params["period"] = s.period

    sql = f"""
    WITH
    millesime AS (
        SELECT year FROM (
            SELECT max(year) AS year FROM zone_exposed.observatory_perimeters WHERE year = %(year)s
            UNION ALL
            SELECT max(year) AS year FROM zone_exposed.observatory_perimeters
            ORDER BY year LIMIT 1
        ) m
    ),
    perims AS (
        SELECT arr AS com FROM zone_exposed.observatory_perimeters
        WHERE year = (SELECT year FROM millesime) AND {col} = %(code)s
    ),
    pts AS (
        SELECT h3_cell_to_parent(start_h3index_z8, %(n)s) AS hex
        FROM zone_exposed.location
        WHERE start_datetime >= %(dt_start)s AND start_datetime < %(dt_end)s
          AND (start_geo_code IN (SELECT com FROM perims) OR end_geo_code IN (SELECT com FROM perims))
        UNION ALL
        SELECT h3_cell_to_parent(end_h3index_z8, %(n)s)
        FROM zone_exposed.location
        WHERE start_datetime >= %(dt_start)s AND start_datetime < %(dt_end)s
          AND (start_geo_code IN (SELECT com FROM perims) OR end_geo_code IN (SELECT com FROM perims))
    ),
    old AS (
        SELECT hex::text AS hex, count(*)::int AS cnt FROM pts GROUP BY hex
    ),
    new AS (
        SELECT h3_cell_to_parent(hex_z8, %(n)s)::text AS hex, sum(count)::int AS cnt
        FROM zone_exposed.location_{s.grain}
        WHERE type = %(type)s AND code = %(code)s AND year = %(year)s {grain_pred}
        GROUP BY 1
    ),
    diff AS (
        SELECT coalesce(o.hex, n.hex) AS hex, o.cnt AS old_cnt, n.cnt AS new_cnt
        FROM old o FULL JOIN new n USING (hex)
        WHERE o.cnt IS DISTINCT FROM n.cnt
    )
    SELECT
        (SELECT count(*) FROM old)                       AS old_hexes,
        (SELECT count(*) FROM new)                       AS new_hexes,
        (SELECT coalesce(sum(cnt),0) FROM old)           AS old_total,
        (SELECT coalesce(sum(cnt),0) FROM new)           AS new_total,
        (SELECT count(*) FROM diff)                      AS mismatched_hexes,
        (SELECT json_agg(d) FROM (SELECT * FROM diff LIMIT 5) d) AS sample
    """
    params["type"] = s.type_
    return sql, params


def main() -> int:
    conn = pg_connect()
    failures = 0
    print(f"{'scope':<34} {'old→new hex':>14} {'old→new tot':>18} {'écart':>8}  verdict")
    print("-" * 92)
    for s in SCOPES:
        sql, params = _old_new_sql(s, N_ZOOM)
        row = conn.execute(sql, params).fetchone()
        old_h, new_h, old_t, new_t, mism, sample = row
        expected = s.code in PLM_COM_CODES and s.type_ == "com"
        if mism == 0:
            verdict = "PASS"
        elif expected:
            verdict = "DIVERGE (attendu)"
        else:
            verdict = "FAIL"
            failures += 1
        label = f"{s.type_}/{s.code}/{s.grain}{'' if s.period is None else '/' + str(s.period)}"
        print(f"{label:<34} {f'{old_h}→{new_h}':>14} {f'{old_t}→{new_t}':>18} {mism:>8}  {verdict}")
        if verdict == "FAIL" and sample:
            print(f"    échantillons: {sample}")
    print("-" * 92)
    print(f"{'OK' if failures == 0 else 'ÉCHEC'} — {failures} scope(s) en FAIL")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
