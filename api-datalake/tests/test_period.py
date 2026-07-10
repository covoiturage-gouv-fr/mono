from datetime import date

from api_datalake.period import get_period_start, is_published, last_record_cutoff


def test_period_start_by_grain():
    assert get_period_start(2026, month=2) == date(2026, 2, 1)
    assert get_period_start(2026, trimester=1) == date(2026, 1, 1)
    assert get_period_start(2026, trimester=3) == date(2026, 7, 1)
    assert get_period_start(2026, semester=2) == date(2026, 7, 1)
    assert get_period_start(2026) == date(2026, 1, 1)


def test_is_published_without_cutoff_is_open():
    # pas de cutoff -> tout est publié (rétrocompatible)
    assert is_published(None, 2026, month=1) is True


def test_is_published_strictly_before_cutoff():
    # cutoff exclusif : 2026-03-01 -> dernier mois visible = février
    assert is_published("2026-03-01", 2026, month=2) is True
    assert is_published("2026-03-01", 2026, month=3) is False


def test_is_published_malformed_cutoff_hides():
    assert is_published("pas-une-date", 2026, month=1) is False


def test_last_record_cutoff_is_exclusive_month():
    # 2026-03-01 -> dernier enregistrement valide = 2026-02
    assert last_record_cutoff("2026-03-01") == (2026, 2)
    # bascule d'année : 2026-01-01 -> 2025-12
    assert last_record_cutoff("2026-01-01") == (2025, 12)


def test_last_record_cutoff_none_when_unset():
    assert last_record_cutoff(None) is None
    assert last_record_cutoff("bad") is None
