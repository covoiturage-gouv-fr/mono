"""Requête de l'index de recherche géographique (index Meilisearch `geo`).

Port de `PopulateGeoCentroid` + `GeoRepositoryProvider.getAllGeo()` côté API
(`api/src/db/geo/datatreatments/PopulateGeoCentroid.ts`,
`api/src/pdc/services/territory/providers/GeoRepositoryProvider.ts`) : aplatit
`zone_trusted.perimeters` (une ligne par commune, colonnes larges com/epci/aom/dep/reg)
en une ligne par territoire de recherche et par millésime (id, territory, l_territory,
type, year, is_latest). Tous les millésimes sont indexés (pas seulement le plus récent)
pour que le dashboard observatoire puisse filtrer sur l'année sélectionnée ; `is_latest`
(calculé via `max(year)`, équivalent de `geo.get_latest_millesime()`) laisse les
consommateurs sans notion d'année (ex. app-partners) filtrer sur le dernier millésime,
comme avant cette évolution.

`type='com'` prend `arr`/`l_arr`, pas `com`/`l_com` : `arr` est la maille la plus fine
(arrondissement pour Paris/Lyon/Marseille, commune IGN partout ailleurs) — même choix
que l'API.
"""

PERIMETERS_TABLE = "zone_trusted.perimeters"

GEO_DOCUMENTS_SQL = f"""
    WITH latest AS (
        SELECT max(year) AS year FROM {PERIMETERS_TABLE}
    ),
    geo_pivot AS (
        SELECT year, 'com' AS type, arr AS territory, l_arr AS l_territory
        FROM {PERIMETERS_TABLE}
        WHERE com IS NOT NULL

        UNION

        SELECT year, 'epci', epci, l_epci
        FROM {PERIMETERS_TABLE}
        WHERE epci IS NOT NULL

        UNION

        SELECT year, 'aom', aom, l_aom
        FROM {PERIMETERS_TABLE}
        WHERE aom IS NOT NULL

        UNION

        SELECT year, 'dep', dep, l_dep
        FROM {PERIMETERS_TABLE}
        WHERE dep IS NOT NULL

        UNION

        SELECT year, 'reg', reg, l_reg
        FROM {PERIMETERS_TABLE}
        WHERE reg IS NOT NULL

        UNION

        SELECT year, 'country', country, l_country
        FROM {PERIMETERS_TABLE}
        WHERE country <> 'XXXXX'
    )
    SELECT
        concat(territory, '_', type, '_', geo_pivot.year) AS id,
        territory,
        l_territory,
        type,
        geo_pivot.year AS year,
        geo_pivot.year = latest.year AS is_latest
    FROM geo_pivot, latest
    ORDER BY type, territory, year
"""
