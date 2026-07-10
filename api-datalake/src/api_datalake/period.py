"""Fenêtre de publication de l'observatoire.

Porté à l'identique de `api/src/pdc/services/observatory/helpers/publishedDate.ts`
et de la logique de cutoff de `LastRecordAction`. Le cutoff (`APP_OBSERVATORY_PUBLISHED_UNTIL`,
format `YYYY-MM-DD`) est une borne supérieure **exclusive**.
"""

from datetime import date


def get_period_start(year: int, month: int | None = None,
                     trimester: int | None = None, semester: int | None = None) -> date:
    """Premier jour de la période demandée (mois > trimestre > semestre > année)."""
    if month is not None:
        return date(year, month, 1)
    if trimester is not None:
        return date(year, (trimester - 1) * 3 + 1, 1)
    if semester is not None:
        return date(year, (semester - 1) * 6 + 1, 1)
    return date(year, 1, 1)


def _parse_cutoff(cutoff: str | None) -> date | None:
    if not cutoff:
        return None
    parts = cutoff.split("-")
    if len(parts) != 3:
        return None
    try:
        y, m, d = (int(p) for p in parts)
        return date(y, m, d)
    except ValueError:
        return None


def is_published(cutoff: str | None, year: int, month: int | None = None,
                 trimester: int | None = None, semester: int | None = None) -> bool:
    """Une période est visible si son début est strictement avant le cutoff.

    Cutoff absent -> tout est publié. Cutoff mal formé -> rien n'est publié.
    """
    if not cutoff:
        return True
    cutoff_date = _parse_cutoff(cutoff)
    if cutoff_date is None:
        return False
    return get_period_start(year, month, trimester, semester) < cutoff_date


def last_record_cutoff(cutoff: str | None) -> tuple[int, int] | None:
    """(year, month) du dernier enregistrement mensuel valide, cutoff exclusif.

    Ex. `2026-03-01` -> `(2026, 2)`. Renvoie None si cutoff absent/invalide.
    """
    cutoff_date = _parse_cutoff(cutoff)
    if cutoff_date is None:
        return None
    year, month = cutoff_date.year, cutoff_date.month
    # mois précédent (le cutoff est exclusif)
    if month == 1:
        return year - 1, 12
    return year, month - 1
