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
    ST_SetSRID(b.geom, 4326) AS geom,
    ST_SetSRID(a.geom, 4326) AS geom_simple,
    ST_SetSRID(c.geom, 4326) AS centroid
  FROM {{ source('raw', 'old_perimeters_simple') }} a
  LEFT JOIN {{ source('raw', 'old_perimeters_full') }} b
    ON a.year = b.year AND a.arr = b.arr
  LEFT JOIN {{ source('raw', 'old_perimeters_centroid') }} c
    ON a.year = c.year AND a.arr = c.arr
),

new_com AS (
  SELECT
    2025 AS year,
    a.com AS arr,
    a.l_com AS l_arr,
    a.com,
    a.l_com,
    a.epci,
    b.l_epci,
    e.aom,
    e.l_aom,
    a.dep,
    c.l_dep,
    a.reg,
    d.l_reg,
    'XXXXX' AS country,
    'France' AS l_country,
    a.population AS pop,
    ST_Area(f.geom::geography) / 1000000 AS surface,
    ST_SetSRID(f.geom, 4326) AS geom,
    ST_SetSRID(a.geom, 4326) AS geom_simple,
    ST_SetSRID(g.geom, 4326) AS centroid
  FROM {{ source('raw', 'ign_aecarto_com_2025') }} a
  LEFT JOIN {{ source('raw', 'ign_aecarto_epci_2025') }} b ON a.epci = b.epci
  LEFT JOIN {{ source('raw', 'ign_aecarto_dep_2025') }} c ON a.dep = c.dep
  LEFT JOIN {{ source('raw', 'ign_aecarto_reg_2025') }} d ON a.reg = d.reg
  LEFT JOIN {{ source('raw', 'cerema_aom_2025') }} e ON a.com = e.code_insee
  LEFT JOIN {{ source('raw', 'ign_ae_com_2025') }} f ON a.com = f.com
  LEFT JOIN {{ source('raw', 'ign_aecentroid_com_2025') }} g ON a.com = g.com
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
    a.population AS pop,
    ST_Area(c.geom::geography) / 1000000 AS surface,
    ST_SetSRID(c.geom, 4326) AS geom,
    ST_SetSRID(a.geom, 4326) AS geom_simple,
    ST_SetSRID(d.geom, 4326) AS centroid
  FROM {{ source('raw', 'ign_aecarto_arr_2025') }} a
  LEFT JOIN new_com b ON a.com = b.arr
  LEFT JOIN {{ source('raw', 'ign_ae_arr_2025') }} c ON a.arr = c.arr
  LEFT JOIN {{ source('raw', 'ign_aecentroid_arr_2025') }} d ON a.arr = d.arr
),

new_country AS (
  SELECT *
  FROM old_perimeters
  WHERE year = 2024
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
  ROW_NUMBER() OVER (ORDER BY year, arr) AS id,
  *
FROM all_perimeters