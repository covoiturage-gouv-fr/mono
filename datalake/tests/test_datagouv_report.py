from datetime import date

from pipelines.helpers.datagouv_report import (
    report_key, debug_csv_key, debug_md_key, build_description, build_report,
)

# Valeurs synthétiques (dépôt public : pas de volumétrie réelle) respectant les invariants :
# total = exposés + retirés ; retirés = start + end - both ; FF + FE + EE = exposés.
STATS = {
    "count_total": 1060,
    "count_exposed": 1000,
    "count_removed": 60,
    "count_removed_start": 40,
    "count_removed_end": 30,
    "count_removed_both": 10,
    # ventilation géographique des exposés (somme = count_exposed)
    "count_exposed_france_france": 700,
    "count_exposed_france_etranger": 200,
    "count_exposed_etranger_etranger": 100,
}


def test_description_french_dates_and_counts():
    d = build_description(date(2026, 6, 1), date(2026, 7, 1), STATS)
    # end exclusif -> dernier jour décrit = 30 juin
    assert "entre le 1 juin 2026 et le 30 juin 2026" in d
    assert "1000" in d
    assert "60 = 40 + 30 - 10" in d


def test_description_geographic_breakdown():
    d = build_description(date(2026, 6, 1), date(2026, 7, 1), STATS)
    assert "France ↔ France : 700" in d
    assert "France ↔ Étranger : 200" in d
    assert "Étranger ↔ Étranger : 100" in d


def test_description_year_rollover_last_day():
    d = build_description(date(2025, 12, 1), date(2026, 1, 1), STATS)
    assert "entre le 1 décembre 2025 et le 31 décembre 2025" in d


def test_report_success_shape():
    r = build_report(
        month="2026-06", start=date(2026, 6, 1), end=date(2026, 7, 1),
        min_occurrences=6, stats=STATS, filename="2026-06.csv",
        status="success", started_at="2026-07-11T00:00:00Z",
        finished_at="2026-07-11T00:05:00Z",
        resource={"id": "abc", "url": "https://data.gouv/x"},
    )
    assert r["status"] == "success"
    assert r["resource"] == {"id": "abc", "url": "https://data.gouv/x"}
    assert r["error"] is None
    assert r["stats"]["count_exposed"] == 1000


def test_report_failure_shape():
    r = build_report(
        month="2026-06", start=date(2026, 6, 1), end=date(2026, 7, 1),
        min_occurrences=6, stats={}, filename="2026-06.csv",
        status="failure", started_at="s", finished_at="f",
        error="upload failed",
    )
    assert r["status"] == "failure"
    assert r["resource"] is None
    assert r["error"] == "upload failed"


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
