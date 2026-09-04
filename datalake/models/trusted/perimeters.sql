{{ config(
  materialized='table',
  tags=['trusted', 'geo', 'perimeters'],
  indexes=[
    { 'columns': ['id'] },
    { 'columns': ['year'] },
    { 'columns': ['arr'] },
    { 'columns': ['aom'] },
    { 'columns': ['epci'] },
    { 'columns': ['surface'] },
    { 'columns': ['valid_from'] },
    { 'columns': ['valid_until'] },
    { 'columns': ['centroid'], 'type': 'gist' },
    { 'columns': ['geom'], 'type': 'gist' },
    { 'columns': ['geom_simple'], 'type': 'gist' },

  ]
) }}

WITH old_perimeters AS (
  SELECT
    a.year,
    a.arr,
    a.l_arr,
    a.com,
    a.l_com,
    a.epci,
    a.l_epci,
    a.aom,
    a.l_aom,
    a.dep,
    a.l_dep,
    a.reg,
    a.l_reg,
    a.country,
    a.l_country,
    a.pop,
    a.surface,
    ST_SETSRID(b.geom, 4326)  AS geom,
    ST_SETSRID(a.geom, 4326)  AS geom_simple,
    ST_SETSRID(c.geom, 4326)  AS centroid,
    MAKE_DATE(a.year, 1, 1)   AS valid_from,
    MAKE_DATE(a.year, 12, 31) AS valid_until
  -- (year, arr) non unique sur l'étranger 2021-2023 (XXXXX = 7 territoires),
  -- la clé est (year, arr, l_arr)
  FROM {{ source('raw', 'old_perimeters_simple') }} AS a
  LEFT JOIN {{ source('raw', 'old_perimeters_full') }} AS b
    ON a.year = b.year AND a.arr = b.arr AND a.l_arr = b.l_arr
  LEFT JOIN {{ source('raw', 'old_perimeters_centroid') }} AS c
    ON a.year = c.year AND a.arr = c.arr AND a.l_arr = c.l_arr
),

new_com AS (
  SELECT
    2025                                 AS year,  -- noqa: RF04
    a.com                                AS arr,
    a.l_com                              AS l_arr,
    a.com,
    a.l_com,
    LEFT(a.epci, 9)                      AS epci,
    b.l_epci,
    e.aom,
    e.l_aom,
    a.dep,
    c.l_dep,
    a.reg,
    d.l_reg,
    'XXXXX'                              AS country,
    'France'                             AS l_country,
    a.population                         AS pop,
    ST_AREA(f.geom::geography) / 1000000 AS surface,
    ST_SETSRID(f.geom, 4326)             AS geom,
    ST_SETSRID(a.geom, 4326)             AS geom_simple,
    ST_SETSRID(g.geom, 4326)             AS centroid,
    MAKE_DATE(2025, 1, 1)                AS valid_from,
    MAKE_DATE(2026, 12, 31)              AS valid_until
  FROM {{ source('raw', 'ign_aecarto_com_2025') }} AS a
  LEFT JOIN
    {{ source('raw', 'ign_aecarto_epci_2025') }} AS b
    ON LEFT(a.epci, 9) = LEFT(b.epci, 9)
  LEFT JOIN {{ source('raw', 'ign_aecarto_dep_2025') }} AS c ON a.dep = c.dep
  LEFT JOIN {{ source('raw', 'ign_aecarto_reg_2025') }} AS d ON a.reg = d.reg
  LEFT JOIN {{ source('raw', 'cerema_aom_2025') }} AS e ON a.com = e.code_insee
  LEFT JOIN {{ source('raw', 'ign_ae_com_2025') }} AS f ON a.com = f.com
  LEFT JOIN {{ source('raw', 'ign_aecentroid_com_2025') }} AS g ON a.com = g.com
),

new_arr AS (
  SELECT
    b.year,
    a.arr,
    a.l_arr,
    a.com,
    b.l_com,
    b.epci,
    b.l_epci,
    b.aom,
    b.l_aom,
    b.dep,
    b.l_dep,
    b.reg,
    b.l_reg,
    b.country,
    b.l_country,
    a.population                         AS pop,
    ST_AREA(c.geom::geography) / 1000000 AS surface,
    ST_SETSRID(c.geom, 4326)             AS geom,
    ST_SETSRID(a.geom, 4326)             AS geom_simple,
    ST_SETSRID(d.geom, 4326)             AS centroid,
    MAKE_DATE(2025, 1, 1)                AS valid_from,
    MAKE_DATE(2026, 12, 31)              AS valid_until
  FROM {{ source('raw', 'ign_aecarto_arr_2025') }} AS a
  LEFT JOIN new_com AS b ON a.com = b.arr
  LEFT JOIN {{ source('raw', 'ign_ae_arr_2025') }} AS c ON a.arr = c.arr
  LEFT JOIN {{ source('raw', 'ign_aecentroid_arr_2025') }} AS d ON a.arr = d.arr
),

new_country AS (
  SELECT
    2025                    AS year,
    arr,
    l_arr,
    com,
    l_com,
    epci,
    l_epci,
    aom,
    l_aom,
    dep,
    l_dep,
    reg,
    l_reg,
    country,
    l_country,
    pop,
    surface,
    geom,
    geom_simple,
    centroid,
    MAKE_DATE(2025, 1, 1)   AS valid_from,
    MAKE_DATE(2026, 12, 31) AS valid_until
  FROM old_perimeters
  WHERE
    year = 2024
    AND country <> 'XXXXX'
),

all_perimeters AS (
  SELECT * FROM old_perimeters
  UNION ALL
  SELECT * FROM new_com
  UNION ALL
  SELECT * FROM new_arr
  UNION ALL
  SELECT * FROM new_country
)

SELECT
  *,
  ROW_NUMBER() OVER (ORDER BY year, arr) AS id
FROM all_perimeters
