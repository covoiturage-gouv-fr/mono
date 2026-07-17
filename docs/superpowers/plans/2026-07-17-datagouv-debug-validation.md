# Mode debug + verdict de cohérence data.gouv — Plan d'implémentation

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ajouter un mode `--debug` à `just datagouv` qui matérialise les artefacts réels (CSV, description, rapport) horodatés sur S3 sans publier, et imprime un verdict d'invariants de cohérence.

**Architecture:** Un module pur `datagouv_checks.py` (aucun réseau/DB, testable en CI) calcule les invariants à partir du `stats` déjà produit et du CSV sur disque. `cmd/datagouv.py` gagne un flag `--debug` qui orchestre le dump S3 + les checks ; les rapports (réel et debug) sont horodatés.

**Tech Stack:** Python 3.13, Typer, psycopg 3, `uv`, pytest. Module `csv` stdlib pour parser le fichier produit.

## Global Constraints

- **Dépôt public / secret des affaires** : aucun nombre de trajets réel ni donnée d'entreprise en dur dans le code source ou les docs commités. Les détails chiffrés des checks sont calculés au **runtime** uniquement (logs du pod + `.md` sur bucket privé), jamais écrits dans les sources/tests (les tests utilisent des fixtures synthétiques).
- **Undercover** : aucune mention Claude/Anthropic dans les commits (pas de trailer `Co-Authored-By`).
- **Commits signés** : `git commit -S` (Yubikey — PIN + touch).
- **Lancer pytest** : depuis `datalake/`, toujours `env -u PYTHONPATH uv run python -m pytest ...` (sans `-u PYTHONPATH`, un `_pytest` du nix-store masque le venv et casse la collecte).
- **TDD** : test rouge → implémentation minimale → vert → commit. Commits fréquents.
- **Contrat CSV** : `;`-délimité, en-tête tout-quoté (`csv_header()`), colonnes texte quotées, numériques nues, NULL = champ vide non quoté. Ordre des colonnes = `DATAGOUV_FIELDS` (`datalake/pipelines/helpers/datagouv_query.py`).

---

## File Structure

- **Create** `datalake/pipelines/helpers/datagouv_checks.py` — checks purs : `CheckResult`, `run_checks`, `has_failure`, `render_markdown`. Une responsabilité : valider la cohérence stats ↔ CSV.
- **Create** `datalake/tests/test_datagouv_checks.py` — tests purs (tournent en CI, sans Postgres).
- **Modify** `datalake/pipelines/helpers/datagouv_report.py` — `build_report` gagne `mode` et `checks` ; nouveaux helpers de nommage d'artefacts horodatés.
- **Modify** `datalake/tests/test_datagouv_report.py` — couverture des nouveaux champs/helpers.
- **Modify** `datalake/pipelines/cmd/datagouv.py` — flag `--debug`, horodatage du rapport, dump S3 CSV/description, câblage verdict + exit code.

---

## Task 1 : Module `datagouv_checks.py` (checks purs)

**Files:**
- Create: `datalake/pipelines/helpers/datagouv_checks.py`
- Test: `datalake/tests/test_datagouv_checks.py`

**Interfaces:**
- Consumes : `DATAGOUV_FIELDS`, `csv_header` depuis `pipelines.helpers.datagouv_query`.
- Produces :
  - `@dataclass(frozen=True) class CheckResult: name: str; level: str; ok: bool; detail: str` (`level` ∈ `{"FAIL","WARN"}`)
  - `run_checks(stats: dict, csv_path: str) -> list[CheckResult]`
  - `has_failure(results: list[CheckResult]) -> bool`
  - `render_markdown(results: list[CheckResult]) -> str`

- [ ] **Step 1 : Écrire les tests rouges**

Créer `datalake/tests/test_datagouv_checks.py` :

```python
import csv
from pipelines.helpers.datagouv_checks import (
    CheckResult, run_checks, has_failure, render_markdown,
)
from pipelines.helpers.datagouv_query import DATAGOUV_FIELDS, csv_header

I = {name: idx for idx, name in enumerate(DATAGOUV_FIELDS)}


def _row(start_insee="35238", end_insee="35047",
         start_dt="2026-05-01T08:00:00+0200",
         start_dep="35", start_town="Rennes", start_tg="Rennes Métropole"):
    r = [""] * len(DATAGOUV_FIELDS)
    r[I["journey_id"]] = "1"
    r[I["journey_start_datetime"]] = start_dt
    r[I["journey_start_insee"]] = start_insee
    r[I["journey_start_department"]] = start_dep
    r[I["journey_start_town"]] = start_town
    r[I["journey_start_towngroup"]] = start_tg
    r[I["journey_end_insee"]] = end_insee
    r[I["journey_end_department"]] = "35"
    r[I["journey_end_town"]] = "Bruz"
    r[I["journey_end_towngroup"]] = "Rennes Métropole"
    return r


def _write_csv(path, rows):
    with open(path, "w", newline="") as f:
        f.write(csv_header() + "\n")
        w = csv.writer(f, delimiter=";")
        for r in rows:
            w.writerow(r)


def _stats(total, exposed, removed, rs, re_, rb, ff, fe, ee):
    return {
        "count_total": total, "count_exposed": exposed, "count_removed": removed,
        "count_removed_start": rs, "count_removed_end": re_, "count_removed_both": rb,
        "count_exposed_france_france": ff,
        "count_exposed_france_etranger": fe,
        "count_exposed_etranger_etranger": ee,
    }


def _by_name(results, name):
    return next(r for r in results if r.name == name)


def test_all_green_no_failure(tmp_path):
    # 2 FF + 1 FE + 1 EE = 4 exposés ; total 5 (1 retiré)
    rows = [
        _row(),                                    # FF
        _row(),                                    # FF
        _row(start_insee="99109", start_dep="", start_town="", start_tg=""),  # FE (départ étranger)
        _row(start_insee="99109", end_insee="99135", start_dep="", start_town="", start_tg=""),  # EE
    ]
    # tri croissant sur journey_start_datetime
    for i, r in enumerate(rows):
        r[I["journey_start_datetime"]] = f"2026-05-01T0{i}:00:00+0200"
    p = tmp_path / "ok.csv"
    _write_csv(p, rows)
    stats = _stats(5, 4, 1, 1, 0, 0, ff=2, fe=1, ee=1)

    results = run_checks(stats, str(p))

    assert not has_failure(results)
    assert all(r.ok for r in results if r.level == "FAIL")


def test_total_partition_failure(tmp_path):
    p = tmp_path / "x.csv"
    _write_csv(p, [_row()])
    stats = _stats(3, 1, 1, 1, 0, 0, ff=1, fe=0, ee=0)  # 1 + 1 != 3
    results = run_checks(stats, str(p))
    r = _by_name(results, "total = exposés + retirés")
    assert r.level == "FAIL" and r.ok is False
    assert has_failure(results)


def test_removed_formula_failure(tmp_path):
    p = tmp_path / "x.csv"
    _write_csv(p, [_row()])
    stats = _stats(2, 1, 1, 1, 1, 1, ff=1, fe=0, ee=0)  # 1 != 1+1-1
    r = _by_name(run_checks(stats, str(p)), "retirés = start + end - both")
    assert r.level == "FAIL" and r.ok is False


def test_geo_sum_failure(tmp_path):
    p = tmp_path / "x.csv"
    _write_csv(p, [_row()])
    stats = _stats(2, 1, 1, 1, 0, 0, ff=0, fe=0, ee=0)  # 0 != exposés 1
    r = _by_name(run_checks(stats, str(p)), "somme ventilation géo = exposés")
    assert r.level == "FAIL" and r.ok is False


def test_csv_rowcount_mismatch_failure(tmp_path):
    p = tmp_path / "x.csv"
    _write_csv(p, [_row(), _row()])  # 2 lignes
    stats = _stats(1, 1, 0, 0, 0, 0, ff=1, fe=0, ee=0)  # exposés 1 != 2
    r = _by_name(run_checks(stats, str(p)), "nb lignes CSV = count_exposed")
    assert r.level == "FAIL" and r.ok is False


def test_geo_from_csv_mismatch_failure(tmp_path):
    p = tmp_path / "x.csv"
    _write_csv(p, [_row(), _row()])  # 2 FF depuis le CSV
    stats = _stats(2, 2, 0, 0, 0, 0, ff=1, fe=1, ee=0)  # stats disent 1 FF / 1 FE
    r = _by_name(run_checks(stats, str(p)), "ventilation géo CSV = stats")
    assert r.level == "FAIL" and r.ok is False


def test_header_mismatch_failure(tmp_path):
    p = tmp_path / "bad_header.csv"
    with open(p, "w", newline="") as f:
        f.write("pas;le;bon;entete\n")
    stats = _stats(0, 0, 0, 0, 0, 0, ff=0, fe=0, ee=0)
    r = _by_name(run_checks(stats, str(p)), "en-tête = contrat")
    assert r.level == "FAIL" and r.ok is False


def test_foreign_row_with_town_failure(tmp_path):
    # ligne étrangère (99xxx) qui a un department/town non vides -> FAIL
    bad = _row(start_insee="99109", start_dep="99", start_town="Kehl", start_tg="X")
    p = tmp_path / "x.csv"
    _write_csv(p, [bad])
    stats = _stats(1, 1, 0, 0, 0, 0, ff=0, fe=1, ee=0)
    r = _by_name(run_checks(stats, str(p)), "lignes étrangères sans libellé FR")
    assert r.level == "FAIL" and r.ok is False


def test_sort_inversions_are_warn_not_failure(tmp_path):
    a = _row(); a[I["journey_start_datetime"]] = "2026-05-01T09:00:00+0200"
    b = _row(); b[I["journey_start_datetime"]] = "2026-05-01T08:00:00+0200"  # < précédent
    p = tmp_path / "x.csv"
    _write_csv(p, [a, b])
    stats = _stats(2, 2, 0, 0, 0, 0, ff=2, fe=0, ee=0)
    results = run_checks(stats, str(p))
    r = _by_name(results, "tri journey_start_datetime")
    assert r.level == "WARN" and r.ok is False
    assert "1" in r.detail                 # 1 inversion comptée
    assert not has_failure(results)        # un WARN n'est pas un FAIL


def test_render_markdown_has_table_rows(tmp_path):
    p = tmp_path / "x.csv"
    _write_csv(p, [_row()])
    stats = _stats(1, 1, 0, 0, 0, 0, ff=1, fe=0, ee=0)
    md = render_markdown(run_checks(stats, str(p)))
    assert "| " in md and "FAIL" not in md.split("\n")[0]  # tableau markdown
    assert "total = exposés + retirés" in md
```

- [ ] **Step 2 : Lancer les tests, vérifier qu'ils échouent**

Run (depuis `datalake/`) :
```bash
env -u PYTHONPATH uv run python -m pytest tests/test_datagouv_checks.py -q
```
Expected : FAIL — `ModuleNotFoundError: No module named 'pipelines.helpers.datagouv_checks'`.

- [ ] **Step 3 : Écrire l'implémentation minimale**

Créer `datalake/pipelines/helpers/datagouv_checks.py` :

```python
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
```

- [ ] **Step 4 : Lancer les tests, vérifier qu'ils passent**

Run :
```bash
env -u PYTHONPATH uv run python -m pytest tests/test_datagouv_checks.py -q
```
Expected : PASS (11 tests).

- [ ] **Step 5 : Commit**

```bash
git add pipelines/helpers/datagouv_checks.py tests/test_datagouv_checks.py
git commit -S -m "feat(datalake): invariants de cohérence de l'export data.gouv"
```

---

## Task 2 : Horodatage du rapport + champs `mode`/`checks`

**Files:**
- Modify: `datalake/pipelines/helpers/datagouv_report.py` (fonction `build_report` ; ajout helpers de nommage)
- Test: `datalake/tests/test_datagouv_report.py`

**Interfaces:**
- Produces :
  - `report_key(month: str, ts: str) -> str` → `"datagouv/logs/<month>-<ts>.json"`
  - `debug_csv_key(month: str, ts: str) -> str` → `"datagouv/logs/<month>-<ts>-debug.csv"`
  - `debug_md_key(month: str, ts: str) -> str` → `"datagouv/logs/<month>-<ts>-debug.md"`
  - `build_report(..., mode: str = "live", checks: list | None = None)` → le dict gagne `"mode"` et `"checks"`.
- Consumes (Task 3) : ces trois helpers + la signature étendue de `build_report`.

- [ ] **Step 1 : Écrire les tests rouges**

Ajouter à `datalake/tests/test_datagouv_report.py` :

```python
from pipelines.helpers.datagouv_report import (
    report_key, debug_csv_key, debug_md_key, build_report,
)
from datetime import date


def test_artifact_keys_are_timestamped():
    assert report_key("2026-07", "20260717T101500Z") == "datagouv/logs/2026-07-20260717T101500Z.json"
    assert debug_csv_key("2026-07", "20260717T101500Z") == "datagouv/logs/2026-07-20260717T101500Z-debug.csv"
    assert debug_md_key("2026-07", "20260717T101500Z") == "datagouv/logs/2026-07-20260717T101500Z-debug.md"


def test_build_report_defaults_to_live_mode_without_checks():
    r = build_report(
        month="2026-07", start=date(2026, 7, 1), end=date(2026, 8, 1), min_occurrences=6,
        stats={}, filename="2026-07.csv", status="success",
        started_at="t0", finished_at="t1",
    )
    assert r["mode"] == "live"
    assert r["checks"] is None


def test_build_report_carries_debug_mode_and_checks():
    checks = [{"name": "total = exposés + retirés", "level": "FAIL", "ok": True, "detail": "..."}]
    r = build_report(
        month="2026-07", start=date(2026, 7, 1), end=date(2026, 8, 1), min_occurrences=6,
        stats={}, filename="2026-07.csv", status="success",
        started_at="t0", finished_at="t1", mode="debug", checks=checks,
    )
    assert r["mode"] == "debug"
    assert r["checks"] == checks
```

- [ ] **Step 2 : Lancer, vérifier l'échec**

Run :
```bash
env -u PYTHONPATH uv run python -m pytest tests/test_datagouv_report.py -q
```
Expected : FAIL — `ImportError: cannot import name 'report_key'` (puis, une fois importé, `KeyError: 'mode'`).

- [ ] **Step 3 : Implémenter**

Dans `datalake/pipelines/helpers/datagouv_report.py`, ajouter en tête (après les imports) :

```python
_LOGS_PREFIX = "datagouv/logs"


def report_key(month: str, ts: str) -> str:
    return f"{_LOGS_PREFIX}/{month}-{ts}.json"


def debug_csv_key(month: str, ts: str) -> str:
    return f"{_LOGS_PREFIX}/{month}-{ts}-debug.csv"


def debug_md_key(month: str, ts: str) -> str:
    return f"{_LOGS_PREFIX}/{month}-{ts}-debug.md"
```

Et modifier la signature + le corps de `build_report` :

```python
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
    mode: str = "live",
    checks: list | None = None,
) -> dict:
    """Rapport d'exécution, écrit en JSON sous `datagouv/logs/<mois>-<ts>.json`."""
    return {
        "month": month,
        "start": start.isoformat(),
        "end": end.isoformat(),
        "min_occurrences": min_occurrences,
        "filename": filename,
        "status": status,
        "mode": mode,
        "started_at": started_at,
        "finished_at": finished_at,
        "stats": stats,
        "checks": checks,
        "resource": {"id": resource.get("id"), "url": resource.get("url")} if resource else None,
        "error": error,
    }
```

- [ ] **Step 4 : Lancer, vérifier le vert**

Run :
```bash
env -u PYTHONPATH uv run python -m pytest tests/test_datagouv_report.py -q
```
Expected : PASS (tests existants + 3 nouveaux).

- [ ] **Step 5 : Commit**

```bash
git add pipelines/helpers/datagouv_report.py tests/test_datagouv_report.py
git commit -S -m "feat(datalake): rapport data.gouv horodaté + champs mode/checks"
```

---

## Task 3 : Flag `--debug` dans `cmd/datagouv.py`

**Files:**
- Modify: `datalake/pipelines/cmd/datagouv.py` (fonctions `write_report`, `run` ; imports)

**Interfaces:**
- Consumes : `run_checks`, `has_failure`, `render_markdown` (Task 1) ; `report_key`, `debug_csv_key`, `debug_md_key`, `build_report` étendu (Task 2) ; `build_description` (existant) ; `s3_upload` (existant).

**Note de vérification :** l'orchestration `--debug` de bout en bout n'est pas testable en CI (S3 + vraies données). Elle est vérifiée **manuellement dans le pod datalake** (Step 4). Les briques pures sont couvertes par Task 1 et Task 2.

- [ ] **Step 1 : Ajouter le timestamp et le nommage horodaté du rapport**

Dans `datalake/pipelines/cmd/datagouv.py` :

Ajouter l'import et un helper timestamp près de `_now_iso` :

```python
from pipelines.helpers.datagouv_report import (
    build_description, build_report, report_key, debug_csv_key, debug_md_key,
)
from pipelines.helpers.datagouv_checks import run_checks, has_failure, render_markdown


def _stamp() -> str:
    return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
```

Remplacer `write_report` pour qu'il écrive sous une **clé** déjà construite (au lieu de `REPORT_PREFIX/<month>.json`) :

```python
def write_report(s3, bucket: str, key: str, report: dict) -> None:
    with tempfile.TemporaryDirectory(prefix="datagouv-report-") as tmp:
        path = os.path.join(tmp, "report.json")
        with open(path, "w") as f:
            json.dump(report, f, ensure_ascii=False, indent=2)
        s3_upload(bucket, key, path, client=s3)
```

Supprimer la constante `REPORT_PREFIX` (remplacée par les helpers de `datagouv_report`).

- [ ] **Step 2 : Ajouter le flag et l'orchestration debug dans `run`**

Modifier la signature de `run` pour ajouter le flag :

```python
    debug: bool = typer.Option(
        False, "--debug",
        help="Ne publie pas ; dépose CSV/description/rapport horodatés sur S3 et imprime le verdict de cohérence.",
    ),
```

Calculer le timestamp juste après avoir déterminé `month` :

```python
    ts = _stamp()
```

Dans le bloc `with tempfile.TemporaryDirectory(...)`, après `stream_csv(...)`, remplacer la branche `if APP_DATAGOUV_UPLOAD ... else ...` par :

```python
                if debug:
                    description = build_description(d_start, d_end, stats)
                    results = run_checks(stats, csv_path)
                    verdict = render_markdown(results)
                    print(verdict)

                    s3_upload(bucket, debug_csv_key(month, ts), csv_path, client=s3)
                    md = description + "\n\n## Verdict de cohérence\n\n" + verdict + "\n"
                    with open(os.path.join(tmp, "desc.md"), "w") as f:
                        f.write(md)
                    s3_upload(bucket, debug_md_key(month, ts), os.path.join(tmp, "desc.md"), client=s3)
                    print(f"🐞 debug : artefacts sur {debug_csv_key(month, ts)} (pas de publication data.gouv)")
                elif os.getenv("APP_DATAGOUV_UPLOAD", "").lower() in ("1", "true", "yes", "on"):
                    client = DataGouvClient(
                        os.environ["APP_DATAGOUV_URL"],
                        os.environ["APP_DATAGOUV_KEY"],
                        os.environ["APP_DATAGOUV_DATASET"],
                    )
                    resource = client.upload(csv_path)
                    client.set_metadata(resource, build_description(d_start, d_end, stats))
                    print(f"✅ resource publiée : {resource.get('id')}")
                else:
                    print("ℹ️ upload data.gouv désactivé (APP_DATAGOUV_UPLOAD) — dry-run")
```

Adapter la construction du rapport de succès pour porter `mode`/`checks` et l'écrire sous la clé horodatée :

```python
        report = build_report(
            month=month, start=d_start, end=d_end, min_occurrences=min_occurrences,
            stats=stats, filename=filename, status="success",
            started_at=started_at, finished_at=_now_iso(), resource=resource,
            mode="debug" if debug else "live",
            checks=[vars(r) for r in results] if debug else None,
        )
        write_report(s3, bucket, report_key(month, ts), report)
        print(f"✅ {filename} — {stats.get('count_exposed')} trajets exposés")

        if debug and has_failure(results):
            raise typer.Exit(code=1)
```

Initialiser `results = None` au début du `try` (à côté de `resource = None`) pour que la branche non-debug reste valide. Dans le `except`, écrire le rapport d'échec sous la clé horodatée également :

```python
        write_report(s3, bucket, report_key(month, ts), report)
```

- [ ] **Step 3 : Vérifier que la suite existante reste verte**

Run :
```bash
env -u PYTHONPATH uv run python -m pytest -q
```
Expected : PASS (les tests purs + les tests pg *skippés* sans Postgres). Aucun test ne référence `REPORT_PREFIX`.

- [ ] **Step 4 : Vérification manuelle (pod datalake)**

Dans le pod datalake, sur une fenêtre récente :
```bash
just datagouv --debug --start 2026-07-01 --end 2026-07-05
```
Attendu :
- le tableau de verdict s'imprime (tous ✅ après le fix comptage ; ⚠️ WARN sur le tri tant que l'offset DROM n'est pas réglé) ;
- `s3://.../datagouv/logs/2026-07-<ts>-debug.csv`, `-debug.md` et `2026-07-<ts>.json` créés ;
- **aucune** publication data.gouv ;
- exit 0 si aucun FAIL.

- [ ] **Step 5 : Commit**

```bash
git add pipelines/cmd/datagouv.py
git commit -S -m "feat(datalake): mode --debug de l'export data.gouv (dump S3 + verdict)"
```

---

## Task 4 : Mise à jour de la documentation

**Files:**
- Modify: `datalake/README.md` (section data.gouv / commandes, si présente)

- [ ] **Step 1 : Documenter le flag**

Ajouter, à l'endroit où la commande `datagouv` est décrite dans `datalake/README.md`, une ligne :

```markdown
- `just datagouv --debug [--start … --end …]` : exécute l'export **sans publier** sur
  data.gouv ; dépose CSV/description/rapport horodatés sous `datagouv/logs/` et imprime un
  verdict d'invariants de cohérence (exit 1 si un invariant dur est cassé). À lancer dans le
  pod datalake pour debugguer sur de vraies données.
```

Si aucune section `datagouv` n'existe dans le README, l'ajouter sous les commandes datalake.

- [ ] **Step 2 : Commit**

```bash
git add datalake/README.md
git commit -S -m "docs(datalake): documente just datagouv --debug"
```

---

## Self-review (couverture spec)

- Flag `--debug`, pas de publication, gate `APP_DATAGOUV_ENABLED` conservée → Task 3.
- Artefacts S3 horodatés (`<mois>-<ts>.json` / `-debug.csv` / `-debug.md`), subsume la todo timestamp → Task 2 + Task 3.
- Rapport enrichi `mode` + `checks` → Task 2.
- Invariants FAIL (partition total, formule retirés, somme géo, nb lignes CSV, ventilation CSV↔stats, en-tête, libellés étrangers) + WARN (tri) → Task 1.
- Exit 1 sur FAIL, dump non interrompu, WARN sans effet sur l'exit → Task 3.
- Module pur testable en CI → Task 1 (aucune dépendance pg).
- k-anonymat par ligne hors périmètre (colonnes absentes du CSV) → non implémenté, conforme à la spec.
- Contrôles métier renvoyés à Elementary (GEN-660) → déjà consignés dans Notion, hors code.
