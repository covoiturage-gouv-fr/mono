import pytest

from pipelines.helpers.export_query import (
    build_copy_sql,
    geo_to_sql,
    operator_to_sql,
    select_columns,
)


def test_operator_excludes_operator_and_has_incentive():
    cols = select_columns("operator")
    assert "operator" not in cols
    assert "has_incentive" not in cols
    assert "incentive_type" in cols
    assert cols[0] == "journey_id"


def test_territory_excludes_has_incentive_only():
    cols = select_columns("territory")
    assert "has_incentive" not in cols
    assert "operator" in cols


def test_operator_to_sql():
    assert operator_to_sql([1, 2]) == "AND operator_id IN (1,2)"
    assert operator_to_sql([]) == ""


def test_geo_to_sql_epci_maps_to_epci_code():
    out = geo_to_sql({"epci": ["200054781"]})
    assert "start_epci_code = '200054781'" in out
    assert "end_epci_code = '200054781'" in out
    assert out.startswith("AND ((")


def test_geo_to_sql_empty():
    assert geo_to_sql(None) == ""
    assert geo_to_sql({"epci": []}) == ""


def test_build_copy_sql_has_date_and_target_predicates():
    sql = build_copy_sql("operator", {
        "start_at": "2026-01-01", "end_at": "2026-02-01",
        "operator_id": [1], "geo_selector": None,
    })
    assert "start_datetime_tz >= '2026-01-01'" in sql
    assert "start_datetime_tz < '2026-02-01'" in sql
    assert "operator_id IN (1)" in sql
    assert "FROM zone_exposed.export_partners" in sql


def test_geo_to_sql_rejects_injection_in_code():
    with pytest.raises(ValueError):
        geo_to_sql({"epci": ["200054781'; DROP TABLE carpools; --"]})


def test_geo_to_sql_rejects_unknown_key():
    with pytest.raises(ValueError):
        geo_to_sql({"evil": ["200054781"]})


def test_build_copy_sql_rejects_bad_date():
    with pytest.raises(ValueError):
        build_copy_sql("operator", {
            "start_at": "2026-01-01'; DROP TABLE carpools; --",
            "end_at": "2026-02-01", "operator_id": [1], "geo_selector": None,
        })


def test_operator_to_sql_rejects_non_int():
    with pytest.raises(ValueError):
        operator_to_sql(["1); DROP TABLE carpools; --"])
