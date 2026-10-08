{{ config(
  materialized='table',
  tags=['exposed', 'observatory', 'perimeters', 'custom'],
  indexes=[
    { 'columns': ['code', 'year', 'arr'], 'unique': true },
  ]
) }}

-- Communes des territoires custom par millésime, pour la résolution
-- territoire -> communes de l'API (pendant de observatory_perimeters).
-- Table et non vue : custom_perimeters est rebâtie chaque jour (DROP … CASCADE).
-- Même règle que custom_perimeters_agg : arr de toutes les versions qui
-- chevauchent l'année.

SELECT DISTINCT
  cp.code,
  y.year,
  cp.arr
FROM {{ ref('custom_perimeters') }} AS cp
INNER JOIN (SELECT DISTINCT year FROM {{ ref('perimeters') }}) AS y
  ON
    cp.valid_from < MAKE_DATE(y.year + 1, 1, 1)
    AND cp.valid_until > MAKE_DATE(y.year, 1, 1)
