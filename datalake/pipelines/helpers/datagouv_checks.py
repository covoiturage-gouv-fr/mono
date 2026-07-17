"""Invariants de cohérence de l'export data.gouv (stats ↔ CSV produit).

Module pur : aucun accès réseau/DB. Prend le `stats` déjà calculé et lit le CSV
sur disque. Réutilisable en garde-fou bloquant avant publication.
"""

import csv
from dataclasses import dataclass

from pipelines.helpers.datagouv_query import DATAGOUV_FIELDS, csv_header

_IDX = {name: i for i, name in enumerate(DATAGOUV_FIELDS)}
_START_INSEE = _IDX["journey_start_insee"]
_END_INSEE = _IDX["journey_end_insee"]
_START_DATETIME = _IDX["journey_start_datetime"]
_START_LABELS = (_IDX["journey_start_department"], _IDX["journey_start_town"], _IDX["journey_start_towngroup"])
_END_LABELS = (_IDX["journey_end_department"], _IDX["journey_end_town"], _IDX["journey_end_towngroup"])

FAIL = "FAIL"
WARN = "WARN"


@dataclass(frozen=True)
class CheckResult:
    name: str
    level: str      # FAIL | WARN
    ok: bool
    detail: str


def _read_csv(csv_path: str) -> tuple[str, list[list[str]]]:
    """Renvoie (ligne d'en-tête brute, lignes de données parsées)."""
    with open(csv_path, newline="") as f:
        content = f.read()
    lines = content.split("\n")
    header = lines[0] if lines else ""
    data_lines = [ln for ln in lines[1:] if ln != ""]
    rows = list(csv.reader(data_lines, delimiter=";"))
    return header, rows


def _is_foreign(code: str) -> bool:
    return code.startswith("99")


def _geo_split(rows: list[list[str]]) -> tuple[int, int, int]:
    ff = fe = ee = 0
    for r in rows:
        sf, ef = _is_foreign(r[_START_INSEE]), _is_foreign(r[_END_INSEE])
        if not sf and not ef:
            ff += 1
        elif sf and ef:
            ee += 1
        else:
            fe += 1
    return ff, fe, ee


def run_checks(stats: dict, csv_path: str) -> list[CheckResult]:
    header, rows = _read_csv(csv_path)
    results: list[CheckResult] = []

    total = stats["count_total"]
    exposed = stats["count_exposed"]
    removed = stats["count_removed"]
    rs, re_, rb = stats["count_removed_start"], stats["count_removed_end"], stats["count_removed_both"]
    ff = stats["count_exposed_france_france"]
    fe = stats["count_exposed_france_etranger"]
    ee = stats["count_exposed_etranger_etranger"]

    results.append(CheckResult(
        "total = exposés + retirés", FAIL, total == exposed + removed,
        f"total={total} exposés={exposed} retirés={removed}"))
    results.append(CheckResult(
        "retirés = start + end - both", FAIL, removed == rs + re_ - rb,
        f"retirés={removed} start={rs} end={re_} both={rb}"))
    results.append(CheckResult(
        "somme ventilation géo = exposés", FAIL, ff + fe + ee == exposed,
        f"FF+FE+EE={ff + fe + ee} exposés={exposed}"))

    results.append(CheckResult(
        "nb lignes CSV = count_exposed", FAIL, len(rows) == exposed,
        f"lignes_csv={len(rows)} exposés={exposed}"))

    csv_ff, csv_fe, csv_ee = _geo_split(rows)
    results.append(CheckResult(
        "ventilation géo CSV = stats", FAIL, (csv_ff, csv_fe, csv_ee) == (ff, fe, ee),
        f"csv=({csv_ff},{csv_fe},{csv_ee}) stats=({ff},{fe},{ee})"))

    results.append(CheckResult(
        "en-tête = contrat", FAIL, header == csv_header(),
        "en-tête conforme" if header == csv_header() else "en-tête différent du contrat"))

    bad_labels = 0
    for r in rows:
        if _is_foreign(r[_START_INSEE]) and any(r[i] for i in _START_LABELS):
            bad_labels += 1
        if _is_foreign(r[_END_INSEE]) and any(r[i] for i in _END_LABELS):
            bad_labels += 1
    results.append(CheckResult(
        "lignes étrangères sans libellé FR", FAIL, bad_labels == 0,
        f"{bad_labels} libellé(s) FR sur point étranger"))

    inversions = sum(1 for a, b in zip(rows, rows[1:]) if b[_START_DATETIME] < a[_START_DATETIME])
    results.append(CheckResult(
        "tri journey_start_datetime", WARN, inversions == 0,
        f"{inversions} inversion(s)"))

    return results


def has_failure(results: list[CheckResult]) -> bool:
    return any(r.level == FAIL and not r.ok for r in results)


def render_markdown(results: list[CheckResult]) -> str:
    lines = ["| Check | Niveau | Verdict | Détail |", "| --- | --- | --- | --- |"]
    for r in results:
        verdict = "✅ OK" if r.ok else ("❌ FAIL" if r.level == FAIL else "⚠️ WARN")
        lines.append(f"| {r.name} | {r.level} | {verdict} | {r.detail} |")
    return "\n".join(lines)
