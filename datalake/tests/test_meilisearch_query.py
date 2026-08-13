from pipelines.helpers.meilisearch_query import GEO_DOCUMENTS_SQL, PERIMETERS_TABLE


def test_reads_only_trusted_perimeters():
    assert PERIMETERS_TABLE in GEO_DOCUMENTS_SQL
    assert "zone_raw." not in GEO_DOCUMENTS_SQL
    assert "zone_aggregated." not in GEO_DOCUMENTS_SQL


def test_indexes_every_millesime_not_only_the_latest():
    # pas de filtre WHERE year = ... sur le pivot : tous les millésimes sont indexés
    assert "max(year)" in GEO_DOCUMENTS_SQL
    assert "WHERE year =" not in GEO_DOCUMENTS_SQL


def test_com_type_uses_arr_not_com():
    # 'arr' est la maille la plus fine (arrondissement PLM / commune ailleurs) ;
    # 'com' fusionne les arrondissements PLM et casserait la granularité de recherche.
    assert "'com' AS type, arr AS territory, l_arr AS l_territory" in GEO_DOCUMENTS_SQL


def test_covers_every_territory_type():
    for type_ in ("com", "epci", "aom", "dep", "reg", "country"):
        assert f"'{type_}'" in GEO_DOCUMENTS_SQL


def test_id_concatenates_territory_type_and_year():
    # l'id doit rester unique alors qu'un territoire a maintenant une ligne par millésime
    assert "concat(territory, '_', type, '_', geo_pivot.year) AS id" in GEO_DOCUMENTS_SQL


def test_is_latest_flags_the_max_year():
    assert "geo_pivot.year = latest.year AS is_latest" in GEO_DOCUMENTS_SQL


def test_orders_by_type_territory_then_year():
    assert "ORDER BY type, territory, year" in GEO_DOCUMENTS_SQL
