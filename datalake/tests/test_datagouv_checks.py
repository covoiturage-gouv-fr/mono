import csv
from pipelines.helpers.datagouv_checks import (
    CheckResult, run_checks, has_failure, render_markdown,
)
from pipelines.helpers.datagouv_query import DATAGOUV_FIELDS, csv_header

I = {name: idx for idx, name in enumerate(DATAGOUV_FIELDS)}


def _row(start_insee="35238", end_insee="35047",
         start_dt="2026-05-01T08:00:00+0200",
         start_dep="35", start_town="Rennes", start_tg="Rennes Métropole",
         end_dep="35", end_town="Bruz", end_tg="Rennes Métropole"):
    r = [""] * len(DATAGOUV_FIELDS)
    r[I["journey_id"]] = "1"
    r[I["journey_start_datetime"]] = start_dt
    r[I["journey_start_insee"]] = start_insee
    r[I["journey_start_department"]] = start_dep
    r[I["journey_start_town"]] = start_town
    r[I["journey_start_towngroup"]] = start_tg
    r[I["journey_end_insee"]] = end_insee
    r[I["journey_end_department"]] = end_dep
    r[I["journey_end_town"]] = end_town
    r[I["journey_end_towngroup"]] = end_tg
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
        _row(start_insee="99109", end_insee="99135", start_dep="", start_town="", start_tg="", end_dep="", end_town="", end_tg=""),  # EE
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
    stats = _stats(3, 1, 2, 1, 1, 1, ff=1, fe=0, ee=0)  # 2 != 1+1-1 = 1
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
