from datetime import date

import pytest

from pipelines.cmd import datagouv as cmd


class FakeCursor:
    def __init__(self, row, cols):
        self._row = row
        self.description = [type("C", (), {"name": c}) for c in cols]

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def execute(self, sql, params):
        self.sql, self.params = sql, params

    def fetchone(self):
        return self._row


class FakeConn:
    def __init__(self, row, cols):
        self._row, self._cols = row, cols

    def cursor(self):
        return FakeCursor(self._row, self._cols)


def test_assert_not_empty_raises_when_no_exposed():
    with pytest.raises(RuntimeError, match="dataset vide"):
        cmd.assert_not_empty({"count_exposed": 0, "count_total": 5}, "2026-06.csv")


def test_assert_not_empty_passes_with_rows():
    cmd.assert_not_empty({"count_exposed": 998086}, "2026-06.csv")  # ne lève pas


def test_fetch_stats_maps_columns_to_dict():
    cols = ["count_total", "count_exposed"]
    conn = FakeConn((1025610, 998086), cols)
    stats = cmd.fetch_stats(conn, date(2026, 6, 1), date(2026, 7, 1), 6)
    assert stats == {"count_total": 1025610, "count_exposed": 998086}
