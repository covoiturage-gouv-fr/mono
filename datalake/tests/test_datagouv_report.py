from datetime import date

from pipelines.helpers.datagouv_report import (
    report_key, debug_csv_key, debug_md_key, build_description, build_report,
)

STATS = {
    "count_total": 1025610,
    "count_exposed": 998086,
    "count_removed": 27524,
    "count_removed_start": 14958,
    "count_removed_end": 15373,
    "count_removed_both": 2807,
    # ventilation géographique des exposés (somme = count_exposed)
    "count_exposed_france_france": 900000,
    "count_exposed_france_etranger": 70000,
    "count_exposed_etranger_etranger": 28086,
}


def test_description_french_dates_and_counts():
    d = build_description(date(2026, 6, 1), date(2026, 7, 1), STATS)
    # end exclusif -> dernier jour décrit = 30 juin
    assert "entre le 1 juin 2026 et le 30 juin 2026" in d
    assert "998086" in d
    assert "27524 = 14958 + 15373 - 2807" in d


def test_description_geographic_breakdown():
    d = build_description(date(2026, 6, 1), date(2026, 7, 1), STATS)
    assert "France ↔ France : 900000" in d
    assert "France ↔ Étranger : 70000" in d
    assert "Étranger ↔ Étranger : 28086" in d


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
    assert r["stats"]["count_exposed"] == 998086


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
