{{ config(
  materialized='table',
  tags=['trusted', 'custom', 'daily'],
  indexes=[
    { 'columns': ['code', 'type', 'year'], 'unique': true },
    { 'columns': ['centroid'], 'type': 'gist' },
    { 'columns': ['geom'], 'type': 'gist' },
  ]
) }}

-- Pendant de perimeters_agg pour les territoires custom (mêmes colonnes) :
-- hors de perimeters_agg, rebâtie au seul millésime, alors que les territoires
-- custom changent à tout moment.
-- Une ligne par millésime où le territoire a une version en vigueur ;
-- membres = arr de toutes les versions qui chevauchent l'année, géométrie de
-- ce millésime.

WITH millesimes AS (
  SELECT DISTINCT year FROM {{ ref('perimeters') }}
),

members AS (
  SELECT DISTINCT
    y.year,
    cp.code,
    cp.arr
  FROM {{ ref('custom_perimeters') }} AS cp
  INNER JOIN millesimes AS y
    ON
      cp.valid_from < MAKE_DATE(y.year + 1, 1, 1)
      AND cp.valid_until > MAKE_DATE(y.year, 1, 1)
)

SELECT
  m.year,
  m.code,
  'custom'                                   AS type,  -- noqa: RF04
  g.name                                     AS libelle,
  ST_MULTI(ST_UNION(p.geom_simple))          AS geom,
  ST_POINTONSURFACE(ST_UNION(p.geom_simple)) AS centroid
FROM members AS m
INNER JOIN {{ ref('perimeters') }} AS p
  ON m.year = p.year AND m.arr = p.arr
INNER JOIN {{ source('dlk_import', 'territory_territory_group') }} AS g
  ON m.code = g._id::varchar
GROUP BY m.year, m.code, g.name
