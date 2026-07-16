"""Description du jeu de données et rapport d'exécution data.gouv.fr."""

from datetime import date, timedelta

_FR_MONTHS = [
    "janvier", "février", "mars", "avril", "mai", "juin",
    "juillet", "août", "septembre", "octobre", "novembre", "décembre",
]


def _fr_date(d: date) -> str:
    return f"{d.day} {_FR_MONTHS[d.month - 1]} {d.year}"


def build_description(start: date, end: date, stats: dict) -> str:
    """Texte FR du jeu de données (port de DataGouvMetadataProvider.description).

    `end` est exclusif : la dernière journée décrite est `end - 1 jour`.
    """
    start_fr = _fr_date(start)
    end_fr = _fr_date(end - timedelta(days=1))
    return f"""
Spécificités jeu de données entre le {start_fr} et le {end_fr} :

Les données concernent également les trajets dont le point de départ OU d'arrivée est situé en dehors du territoire français.

- Nombre trajets collectés et validés par le registre de preuve de covoiturage {stats['count_total']}.
- Nombre de trajets exposés dans le jeu de données : {stats['count_exposed']}.
- Nombre de trajets supprimés du jeu de données : {stats['count_removed']} = {stats['count_removed_start']} + {stats['count_removed_end']} - {stats['count_removed_both']}.
  - Nombre de trajets dont l'occurrence du code INSEE de départ est < 6 : {stats['count_removed_start']}
  - Nombre de trajets dont l'occurrence du code INSEE d'arrivée est < 6 : {stats['count_removed_end']}
  - Nombre de trajets dont l'occurrence du code INSEE de départ ET d'arrivée est < 6 : {stats['count_removed_both']}

Répartition géographique des trajets exposés (un point hors territoire français = code INSEE 99xxx) :
- Trajets France ↔ France : {stats['count_exposed_france_france']}
- Trajets France ↔ Étranger : {stats['count_exposed_france_etranger']}
- Trajets Étranger ↔ Étranger : {stats['count_exposed_etranger_etranger']}
    """.strip()


def build_report(
    *,
    month: str,
    start: date,
    end: date,
    min_occurrences: int,
    stats: dict,
    filename: str,
    status: str,
    started_at: str,
    finished_at: str,
    resource: dict | None = None,
    error: str | None = None,
) -> dict:
    """Rapport d'exécution, écrit en JSON sous `datagouv/logs/<mois>.json`."""
    return {
        "month": month,
        "start": start.isoformat(),
        "end": end.isoformat(),
        "min_occurrences": min_occurrences,
        "filename": filename,
        "status": status,
        "started_at": started_at,
        "finished_at": finished_at,
        "stats": stats,
        "resource": {"id": resource.get("id"), "url": resource.get("url")} if resource else None,
        "error": error,
    }
