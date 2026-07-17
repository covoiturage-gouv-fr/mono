"""Tests de sortie réelle du job data.gouv (exécutent le SQL/COPY sur un Postgres).

Ils remplacent le gate de migration « diff octet vs legacy » (one-shot) par un
garde-fou permanent sur le contrat publié : quoting, colonnes numériques nues,
NULL vides (trajets étrangers), tri, filtre k-anonymat et ventilation géographique.

Sautés si aucun Postgres n'est configuré (cf. fixture `pg`).
"""

import os
import tempfile
from datetime import date

from pipelines.cmd.datagouv import fetch_stats, stream_csv
from pipelines.helpers.datagouv_query import csv_header

# (nom, type) des colonnes de la vue `zone_exposed.export_opendata` utiles au COPY :
# le contrat publié + les 3 colonnes servant au filtre (date, compteurs k-anon).
EXPORT_COLS = [
    ("journey_id", "integer"), ("trip_id", "text"),
    ("journey_start_datetime", "text"), ("journey_start_date", "text"), ("journey_start_time", "text"),
    ("journey_start_lon", "double precision"), ("journey_start_lat", "double precision"),
    ("journey_start_insee", "text"), ("journey_start_department", "text"), ("journey_start_town", "text"),
    ("journey_start_towngroup", "text"), ("journey_start_country", "text"),
    ("journey_end_datetime", "text"), ("journey_end_date", "text"), ("journey_end_time", "text"),
    ("journey_end_lon", "double precision"), ("journey_end_lat", "double precision"),
    ("journey_end_insee", "text"), ("journey_end_department", "text"), ("journey_end_town", "text"),
    ("journey_end_towngroup", "text"), ("journey_end_country", "text"),
    ("passenger_seats", "integer"), ("operator_class", "text"),
    ("journey_distance", "integer"), ("journey_duration", "integer"), ("has_incentive", "text"),
    ("start_date_filter", "date"), ("start_insee_count", "integer"), ("end_insee_count", "integer"),
]
_NAMES = [n for n, _ in EXPORT_COLS]


def _seed_export(conn, rows):
    conn.execute("CREATE SCHEMA IF NOT EXISTS zone_exposed")
    conn.execute("DROP TABLE IF EXISTS zone_exposed.export_opendata")
    ddl = ", ".join(f'"{n}" {t}' for n, t in EXPORT_COLS)
    conn.execute(f"CREATE TABLE zone_exposed.export_opendata ({ddl})")
    collist = ", ".join(f'"{n}"' for n in _NAMES)
    placeholders = ", ".join(["%s"] * len(_NAMES))
    with conn.cursor() as cur:
        cur.executemany(
            f"INSERT INTO zone_exposed.export_opendata ({collist}) VALUES ({placeholders})",
            [tuple(r[n] for n in _NAMES) for r in rows],
        )


def _fr_row(jid, trip, hhmm, insee, dep, town, tg, seats, oclass, dist, dur, inc, sic, eic):
    d = "2026-05-01"
    return {
        "journey_id": jid, "trip_id": trip,
        "journey_start_datetime": f"{d}T{hhmm}:00+0200", "journey_start_date": d, "journey_start_time": f"{hhmm}:00",
        "journey_start_lon": 1.5, "journey_start_lat": 48.0,
        "journey_start_insee": insee, "journey_start_department": dep, "journey_start_town": town,
        "journey_start_towngroup": tg, "journey_start_country": "France",
        "journey_end_datetime": f"{d}T{hhmm}:00+0200", "journey_end_date": d, "journey_end_time": f"{hhmm}:00",
        "journey_end_lon": 1.6, "journey_end_lat": 49.0,
        "journey_end_insee": "35047", "journey_end_department": "35", "journey_end_town": "Bruz",
        "journey_end_towngroup": tg, "journey_end_country": "France",
        "passenger_seats": seats, "operator_class": oclass,
        "journey_distance": dist, "journey_duration": dur, "has_incentive": inc,
        "start_date_filter": date(2026, 5, 1), "start_insee_count": sic, "end_insee_count": eic,
    }


def _read(path):
    with open(path, "rb") as f:
        return f.read().decode("utf-8")


def test_csv_output_matches_published_contract(pg):
    # B (08:00) et A (09:00) exposés ; C étranger (dep/town/towngroup NULL) ; D exclu (k-anon).
    b = _fr_row(2, "bbbb", "08:00", "35238", "35", "Rennes", "Rennes Métropole", 1, "C", 12000, 30, "NON", 10, 8)
    a = _fr_row(1, "aaaa", "09:00", "35238", "35", "Rennes", "Rennes Métropole", 2, "C", 8000, 20, "OUI", 6, 6)
    d = _fr_row(4, "dddd", "07:00", "35238", "35", "Rennes", "Rennes Métropole", 1, "C", 9000, 22, "NON", 5, 10)
    c = {
        "journey_id": 3, "trip_id": "cccc",
        "journey_start_datetime": "2026-05-01T10:00:00+0200", "journey_start_date": "2026-05-01", "journey_start_time": "10:00:00",
        "journey_start_lon": 9.1, "journey_start_lat": 49.1,
        "journey_start_insee": "99109", "journey_start_department": None, "journey_start_town": None,
        "journey_start_towngroup": None, "journey_start_country": "Allemagne",
        "journey_end_datetime": "2026-05-01T10:10:00+0200", "journey_end_date": "2026-05-01", "journey_end_time": "10:10:00",
        "journey_end_lon": 9.2, "journey_end_lat": 49.2,
        "journey_end_insee": "99135", "journey_end_department": None, "journey_end_town": None,
        "journey_end_towngroup": None, "journey_end_country": "Allemagne",
        "passenger_seats": 1, "operator_class": "C",
        "journey_distance": 5000, "journey_duration": 15, "has_incentive": "NON",
        "start_date_filter": date(2026, 5, 1), "start_insee_count": 6, "end_insee_count": 6,
    }
    _seed_export(pg, [a, b, c, d])

    with tempfile.TemporaryDirectory() as tmp:
        path = os.path.join(tmp, "out.csv")
        stream_csv(pg, date(2026, 5, 1), date(2026, 6, 1), 6, path)
        out = _read(path)

    lines = out.split("\n")
    data = [ln for ln in lines[1:] if ln != ""]

    # en-tête tout-quoté, à l'octet
    assert lines[0] == csv_header()
    # D exclu par le k-anon (start_insee_count = 5 < 6) ; tri intra-jour B(08:00), A(09:00), C(10:00)
    assert len(data) == 3
    assert [ln.split(";")[0] for ln in data] == ["2", "1", "3"]

    # ligne France : journey_id/lon/lat/seats/distance/duration nus, texte quoté
    assert data[0] == (
        '2;"bbbb";"2026-05-01T08:00:00+0200";"2026-05-01";"08:00:00";1.5;48;'
        '"35238";"35";"Rennes";"Rennes Métropole";"France";'
        '"2026-05-01T08:00:00+0200";"2026-05-01";"08:00:00";1.6;49;'
        '"35047";"35";"Bruz";"Rennes Métropole";"France";1;"C";12000;30;"NON"'
    )
    # ligne étranger : department/town/towngroup NULL -> vides NON quotés (`;;;`)
    assert data[2] == (
        '3;"cccc";"2026-05-01T10:00:00+0200";"2026-05-01";"10:00:00";9.1;49.1;'
        '"99109";;;;"Allemagne";'
        '"2026-05-01T10:10:00+0200";"2026-05-01";"10:10:00";9.2;49.2;'
        '"99135";;;;"Allemagne";1;"C";5000;15;"NON"'
    )


def _seed_stats(conn, carpools, agg_from, agg_to):
    conn.execute("SET TIME ZONE 'UTC'")
    conn.execute("CREATE SCHEMA IF NOT EXISTS zone_trusted")
    conn.execute("CREATE SCHEMA IF NOT EXISTS zone_aggregated")
    conn.execute("DROP TABLE IF EXISTS zone_trusted.carpools")
    conn.execute(
        "CREATE TABLE zone_trusted.carpools ("
        "start_geo_code text, end_geo_code text, "
        "start_datetime timestamptz, valid_acquisition_status boolean)"
    )
    for tbl in ("territory_month_arr_from", "territory_month_arr_to"):
        conn.execute(f"DROP TABLE IF EXISTS zone_aggregated.{tbl}")
        conn.execute(
            f"CREATE TABLE zone_aggregated.{tbl} "
            "(code text, incremental_date timestamptz, carpools integer)"
        )
    with conn.cursor() as cur:
        cur.executemany(
            "INSERT INTO zone_trusted.carpools VALUES (%s, %s, %s, %s)", carpools
        )
        cur.executemany(
            "INSERT INTO zone_aggregated.territory_month_arr_from VALUES (%s, %s, %s)", agg_from
        )
        cur.executemany(
            "INSERT INTO zone_aggregated.territory_month_arr_to VALUES (%s, %s, %s)", agg_to
        )


def test_stats_geographic_breakdown(pg):
    m = "2026-05-15 10:00:00+00"      # trajet du mois de mai
    month = "2026-05-01 00:00:00+00"  # bucket mensuel (UTC)
    carpools = [
        ("35238", "35047", m, True),   # FF exposé
        ("99109", "35238", m, True),   # FE exposé
        ("99109", "99135", m, True),   # EE exposé
        ("35238", "35999", m, True),   # retiré (arrivée < 6)
        ("35238", "35047", m, False),  # invalide -> hors total
    ]
    agg_from = [("35238", month, 10), ("99109", month, 7)]
    agg_to = [("35047", month, 8), ("35238", month, 9), ("99135", month, 6), ("35999", month, 3)]
    _seed_stats(pg, carpools, agg_from, agg_to)

    stats = fetch_stats(pg, date(2026, 5, 1), date(2026, 6, 1), 6)

    assert stats["count_total"] == 4          # les 4 valides
    assert stats["count_exposed"] == 3
    assert stats["count_removed"] == 1
    assert stats["count_exposed_france_france"] == 1
    assert stats["count_exposed_france_etranger"] == 1
    assert stats["count_exposed_etranger_etranger"] == 1
    # la ventilation partitionne les exposés
    assert (
        stats["count_exposed_france_france"]
        + stats["count_exposed_france_etranger"]
        + stats["count_exposed_etranger_etranger"]
        == stats["count_exposed"]
    )


def test_stats_counts_missing_aggregate_as_removed(pg):
    # Un geo_code absent de l'agrégat territorial (jointure NULL) est non vérifiable
    # -> retiré, jamais perdu : total = exposés + retirés doit rester vrai.
    m = "2026-05-15 10:00:00+00"
    month = "2026-05-01 00:00:00+00"
    carpools = [
        ("35238", "35047", m, True),   # exposé (les deux >= 6)
        ("45308", "35047", m, True),   # départ absent de l'agrégat -> ts NULL
    ]
    agg_from = [("35238", month, 10)]  # 45308 volontairement absent
    agg_to = [("35047", month, 8)]
    _seed_stats(pg, carpools, agg_from, agg_to)

    stats = fetch_stats(pg, date(2026, 5, 1), date(2026, 6, 1), 6)

    assert stats["count_total"] == 2
    assert stats["count_exposed"] == 1
    assert stats["count_removed"] == 1
    assert stats["count_removed_start"] == 1
    assert stats["count_removed_end"] == 0
    assert stats["count_total"] == stats["count_exposed"] + stats["count_removed"]
