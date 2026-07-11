from datetime import date

from pipelines.helpers.datagouv_query import (
    DATAGOUV_FIELDS,
    build_opendata_copy_sql,
    build_stats_sql,
    default_window,
)


def test_default_window_is_previous_month():
    assert default_window(date(2026, 7, 11)) == (date(2026, 6, 1), date(2026, 7, 1))


def test_default_window_year_rollover():
    assert default_window(date(2026, 1, 15)) == (date(2025, 12, 1), date(2026, 1, 1))


def test_copy_sql_projects_contract_columns_in_order():
    sql, params = build_opendata_copy_sql(date(2026, 6, 1), date(2026, 7, 1), 6)
    assert "SELECT *" not in sql
    # ordre du contrat préservé : chaque colonne apparaît après la précédente
    idx = [sql.index(c) for c in DATAGOUV_FIELDS]
    assert idx == sorted(idx)
    assert DATAGOUV_FIELDS[0] == "journey_id"
    assert DATAGOUV_FIELDS[-1] == "has_incentive"


def test_copy_sql_applies_kanon_and_month_filter():
    sql, params = build_opendata_copy_sql(date(2026, 6, 1), date(2026, 7, 1), 6)
    assert "start_insee_count >= %(min_occ)s" in sql
    assert "end_insee_count >= %(min_occ)s" in sql
    assert "start_date_filter >= %(start)s" in sql
    assert "start_date_filter < %(end)s" in sql
    assert "ORDER BY start_date_filter ASC" in sql
    assert params == {"start": date(2026, 6, 1), "end": date(2026, 7, 1), "min_occ": 6}


def test_copy_sql_reads_only_exposed_zone():
    sql, _ = build_opendata_copy_sql(date(2026, 6, 1), date(2026, 7, 1), 6)
    assert "zone_exposed.export_opendata" in sql
    assert "zone_raw." not in sql


def test_stats_sql_is_light_no_positions():
    sql, params = build_stats_sql(date(2026, 6, 1), date(2026, 7, 1), 6)
    # requête allégée : trusted + agrégés, jamais la vue lourde ni les positions GPS
    assert "zone_exposed.export_opendata" not in sql
    assert "start_position" not in sql and "st_x" not in sql.lower()
    assert "zone_trusted.carpools" in sql
    assert "territory_month_arr_from" in sql
    assert "territory_month_arr_to" in sql


def test_stats_sql_counts_match_inclusion_exclusion_semantics():
    sql, _ = build_stats_sql(date(2026, 6, 1), date(2026, 7, 1), 6)
    for col in ("count_total", "count_exposed", "count_removed",
                "count_removed_start", "count_removed_end", "count_removed_both"):
        assert col in sql
    assert "c.valid_acquisition_status" in sql
