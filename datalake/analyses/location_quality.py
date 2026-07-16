#!/usr/bin/env python3
"""Contrôles qualité de la heatmap `location` pré-agrégée (PR modèles).

À lancer APRÈS le backfill des modèles `location_*`, avant la bascule de l'API.
Complète le harnais de parité (`location_parity.py`, qui compare à l'ancienne
requête) par des invariants internes qui ne dépendent PAS de l'ancienne vue —
donc encore valables après sa suppression.

Contrôle central : **cohérence des grains grossiers vs mois**. Les agrégats
`*_{quarter,semester,year}` sont construits en `delete+insert` sur un bucket de
période ; un backfill mené mois par mois peut ne laisser que le dernier mois dans
le bucket (bug connu, cf. suivi Data). Ce script le détecte : la somme d'une
période doit égaler la somme de ses mois constitutifs.

    DBT_HOST=... DBT_PORT=... DBT_USER=... DBT_PASSWORD=... DBT_DBNAME=datalake \
        python analyses/location_quality.py

Code retour != 0 si un contrôle échoue.
"""

from __future__ import annotations

import sys

from pipelines.helpers.pg import pg_connect

# Échantillon (type, code, année) : un fin, un moyen, un dense. À adapter au backfill.
SAMPLE = [
    ("com", "31555", 2024),
    ("reg", "11", 2024),
    ("country", "XXXXX", 2024),  # France (cf. suivi Data : XXXXX = code pays France)
]


def _sum(conn, table: str, where: str, params: dict) -> int:
    row = conn.execute(f"SELECT coalesce(sum(count), 0) FROM zone_exposed.{table} WHERE {where}",
                       params).fetchone()
    return int(row[0])


def _neg_counts(conn, table: str) -> int:
    return int(conn.execute(f"SELECT count(*) FROM zone_exposed.{table} WHERE count < 0").fetchone()[0])


def check_scope(conn, type_: str, code: str, year: int) -> list[tuple[str, bool, str]]:
    """Renvoie une liste de (libellé, ok, détail) pour un scope."""
    base = {"type": type_, "code": code, "year": year}
    w = "type = %(type)s AND code = %(code)s AND year = %(year)s"
    out: list[tuple[str, bool, str]] = []

    months = _sum(conn, "location_month", w, base)

    # 1. année == somme des mois de l'année
    y = _sum(conn, "location_year", w, base)
    out.append((f"year == Σmois", y == months, f"year={y} vs Σmois={months}"))

    # 2. chaque trimestre == somme de ses 3 mois
    for q in (1, 2, 3, 4):
        m0 = (q - 1) * 3 + 1
        qsum = _sum(conn, "location_quarter", w + " AND quarter = %(q)s", {**base, "q": q})
        msum = _sum(conn, "location_month",
                    w + " AND month BETWEEN %(a)s AND %(b)s", {**base, "a": m0, "b": m0 + 2})
        out.append((f"Q{q} == ΣmoisQ{q}", qsum == msum, f"q={qsum} vs Σmois={msum}"))

    # 3. chaque semestre == somme de ses 6 mois
    for s, (a, b) in ((1, (1, 6)), (2, (7, 12))):
        ssum = _sum(conn, "location_semester", w + " AND semester = %(s)s", {**base, "s": s})
        msum = _sum(conn, "location_month",
                    w + " AND month BETWEEN %(a)s AND %(b)s", {**base, "a": a, "b": b})
        out.append((f"S{s} == ΣmoisS{s}", ssum == msum, f"s={ssum} vs Σmois={msum}"))

    return out


def main() -> int:
    conn = pg_connect()
    failures = 0

    # Sanité structurelle globale : aucun count négatif.
    for grain in ("month", "quarter", "semester", "year"):
        n = _neg_counts(conn, f"location_{grain}")
        status = "OK" if n == 0 else "FAIL"
        if n:
            failures += 1
        print(f"[structure] location_{grain} counts<0 : {n}  {status}")

    print("-" * 78)
    for type_, code, year in SAMPLE:
        print(f"### {type_}/{code}/{year}")
        for label, ok, detail in check_scope(conn, type_, code, year):
            verdict = "OK" if ok else "FAIL"
            if not ok:
                failures += 1
            print(f"  {label:<16} {verdict:<5} {detail}")
    print("-" * 78)
    print(f"{'OK' if failures == 0 else 'ÉCHEC'} — {failures} contrôle(s) en FAIL")
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
