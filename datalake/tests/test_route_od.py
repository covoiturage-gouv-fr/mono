import uuid

import pytest

from pipelines.cmd.route_od import batched, ensure_tables, missing_pairs, node_rows, route_row, write_batch
from pipelines.helpers.osrm import Pair, Route

PAIR = Pair(2.308475, 48.850452, 2.35, 48.85)
ROUTE = Route(
    nodes=[10, 11, 12, 11],
    coords=[(2.3, 48.8), (2.31, 48.81), (2.32, 48.82), (2.31, 48.81)],
    distance=3969.2,
    duration=657.6,
)


def test_batched_splits_and_keeps_last_partial_chunk():
    assert list(batched(range(5), 2)) == [[0, 1], [2, 3], [4]]


def test_route_row_for_a_found_route():
    assert route_row("2023-12-21", PAIR, ROUTE) == (
        "2023-12-21", 2.308475, 48.850452, 2.35, 48.85, "ok", 3969.2, 657.6, [10, 11, 12, 11],
    )


def test_route_row_keeps_pairs_without_route_so_they_are_not_retried():
    assert route_row("2023-12-21", PAIR, None) == (
        "2023-12-21", 2.308475, 48.850452, 2.35, 48.85, "no_route", None, None, None,
    )


def test_node_rows_are_deduplicated_across_routes():
    other = Route(nodes=[12, 13], coords=[(2.32, 48.82), (2.33, 48.83)], distance=1, duration=1)

    rows = node_rows("2023-12-21", [ROUTE, None, other])

    assert sorted(rows) == [
        ("2023-12-21", 10, 2.3, 48.8),
        ("2023-12-21", 11, 2.31, 48.81),
        ("2023-12-21", 12, 2.32, 48.82),
        ("2023-12-21", 13, 2.33, 48.83),
    ]


@pytest.fixture
def cache(pg):
    ensure_tables(pg)
    ensure_tables(pg)  # rejouable à chaque lancement
    dataset = f"test-{uuid.uuid4()}"
    yield pg, dataset
    pg.execute("DELETE FROM zone_raw.osrm_route WHERE dataset = %s", (dataset,))
    pg.execute("DELETE FROM zone_raw.osrm_node WHERE dataset = %s", (dataset,))


def test_write_batch_is_idempotent(cache):
    pg, dataset = cache
    other = Pair(1.0, 45.0, 1.1, 45.1)

    write_batch(pg, dataset, [(PAIR, ROUTE), (other, None)])
    write_batch(pg, dataset, [(PAIR, ROUTE), (other, None)])

    routes = pg.execute(
        "SELECT status, nodes FROM zone_raw.osrm_route WHERE dataset = %s ORDER BY status",
        (dataset,),
    ).fetchall()
    assert routes == [("no_route", None), ("ok", [10, 11, 12, 11])]
    n_nodes = pg.execute("SELECT count(*) FROM zone_raw.osrm_node WHERE dataset = %s", (dataset,)).fetchone()[0]
    assert n_nodes == 3


def test_missing_pairs_skips_pairs_already_in_cache(cache, monkeypatch):
    pg, dataset = cache
    od = [PAIR, Pair(1.0, 45.0, 1.1, 45.1)]
    monkeypatch.setattr("pipelines.cmd.route_od.PAIRS_SQL", _values_sql(od))

    write_batch(pg, dataset, [(PAIR, ROUTE)])

    assert missing_pairs(pg, dataset) == [Pair(1.0, 45.0, 1.1, 45.1)]


def _values_sql(pairs):
    values = ", ".join(f"({p.lng_1}, {p.lat_1}, {p.lng_2}, {p.lat_2})" for p in pairs)
    return (
        "SELECT lng_1::numeric(9, 6), lat_1::numeric(9, 6), lng_2::numeric(9, 6), lat_2::numeric(9, 6) "
        f"FROM (VALUES {values}) AS od (lng_1, lat_1, lng_2, lat_2)"
    )
