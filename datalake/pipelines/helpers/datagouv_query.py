"""Requêtes de la publication open-data data.gouv.fr.

Deux requêtes pures (SQL + params), testables sans base :

- `build_opendata_copy_sql` : le CSV publié. Lit la vue `zone_exposed.export_opendata`
  (non matérialisée), projette les colonnes du contrat dans l'ordre exact de l'ancien
  `datagouvListQuery.ts`, applique le filtre k-anonymat (occurrence INSEE >= min).
- `build_stats_sql` : les compteurs (total / exposés / retirés). Volontairement **allégée** :
  elle recompte les occurrences INSEE via `zone_trusted.carpools` x les agrégats
  `territory_month_arr_{from,to}` (matérialisés), **sans** la jointure FDW des positions GPS
  que porte `export_opendata`. On ne re-scanne donc pas la vue lourde pour compter.

Les valeurs (dates du mois, min_occurrences) sont calculées par le job — pas des entrées
utilisateur — et passées en **paramètres** psycopg (`%(...)s`).
"""

from datetime import date

OPENDATA_TABLE = "zone_exposed.export_opendata"
CARPOOLS_TABLE = "zone_trusted.carpools"
TERRITORY_FROM = "zone_aggregated.territory_month_arr_from"
TERRITORY_TO = "zone_aggregated.territory_month_arr_to"

# Ordre des colonnes = ordre des colonnes du CSV publié (port de config/datagouv.ts
# `fields` et de datagouvListQuery.ts). NE PAS réordonner sans revalider le contrat.
DATAGOUV_FIELDS = [
    "journey_id",
    "trip_id",
    "journey_start_datetime",
    "journey_start_date",
    "journey_start_time",
    "journey_start_lon",
    "journey_start_lat",
    "journey_start_insee",
    "journey_start_department",
    "journey_start_town",
    "journey_start_towngroup",
    "journey_start_country",
    "journey_end_datetime",
    "journey_end_date",
    "journey_end_time",
    "journey_end_lon",
    "journey_end_lat",
    "journey_end_insee",
    "journey_end_department",
    "journey_end_town",
    "journey_end_towngroup",
    "journey_end_country",
    "passenger_seats",
    "operator_class",
    "journey_distance",
    "journey_duration",
    "has_incentive",
]


def default_window(today: date) -> tuple[date, date]:
    """Fenêtre par défaut = le mois précédent.

    Renvoie (premier jour du mois précédent, premier jour du mois courant).
    """
    end = today.replace(day=1)  # premier jour du mois courant (borne exclusive)
    prev_year = end.year - 1 if end.month == 1 else end.year
    prev_month = 12 if end.month == 1 else end.month - 1
    start = date(prev_year, prev_month, 1)
    return start, end


def build_opendata_copy_sql(start: date, end: date, min_occurrences: int) -> tuple[str, dict]:
    """SELECT interne du CSV open-data (à envelopper dans un COPY ... TO STDOUT).

    Filtre : mois [start, end) sur `start_date_filter`, et k-anonymat sur les
    compteurs d'occurrence INSEE déjà exposés par la vue.
    """
    cols = ",\n      ".join(DATAGOUV_FIELDS)
    sql = f"""
    SELECT
      {cols}
    FROM {OPENDATA_TABLE}
    WHERE start_date_filter >= %(start)s
      AND start_date_filter < %(end)s
      AND start_insee_count >= %(min_occ)s
      AND end_insee_count >= %(min_occ)s
    ORDER BY start_date_filter ASC
    """
    return sql, {"start": start, "end": end, "min_occ": min_occurrences}


def build_stats_sql(start: date, end: date, min_occurrences: int) -> tuple[str, dict]:
    """Compteurs total / exposés / retirés du mois (requête allégée, sans positions).

    Sémantique identique à l'ancien `datagouvStatsQuery` (insee_counters) :
    `count_removed = count_removed_start + count_removed_end - count_removed_both`.
    """
    sql = f"""
    SELECT
      count(*) AS count_total,
      count(*) FILTER (
        WHERE ts.carpools >= %(min_occ)s AND te.carpools >= %(min_occ)s
      ) AS count_exposed,
      count(*) FILTER (
        WHERE ts.carpools < %(min_occ)s OR te.carpools < %(min_occ)s
      ) AS count_removed,
      count(*) FILTER (WHERE ts.carpools < %(min_occ)s) AS count_removed_start,
      count(*) FILTER (WHERE te.carpools < %(min_occ)s) AS count_removed_end,
      count(*) FILTER (
        WHERE ts.carpools < %(min_occ)s AND te.carpools < %(min_occ)s
      ) AS count_removed_both
    FROM {CARPOOLS_TABLE} AS c
    LEFT JOIN {TERRITORY_FROM} AS ts
      ON c.start_geo_code = ts.code
      AND date_trunc('month', c.start_datetime) = ts.incremental_date
    LEFT JOIN {TERRITORY_TO} AS te
      ON c.end_geo_code = te.code
      AND date_trunc('month', c.start_datetime) = te.incremental_date
    WHERE c.valid_acquisition_status
      AND (c.start_datetime AT TIME ZONE 'Europe/Paris')::date >= %(start)s
      AND (c.start_datetime AT TIME ZONE 'Europe/Paris')::date < %(end)s
    """
    return sql, {"start": start, "end": end, "min_occ": min_occurrences}
