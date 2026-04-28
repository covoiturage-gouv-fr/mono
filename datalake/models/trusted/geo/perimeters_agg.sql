{{ config(
  materialized='table',
  tags=['trusted', 'geo', 'perimeters_agg'],
  indexes=[
    { 'columns': ['code', 'type', 'year'] },
    { 'columns': ['centroid'], 'type': 'gist' },
    { 'columns': ['geom'], 'type': 'gist' },
  ]
) }}

SELECT
  year,
  arr AS code,
  'com' AS type,
  l_arr       AS libelle,
  geom_simple AS geom,
  centroid
FROM {{ ref('perimeters') }}
WHERE arr IS NOT NULL
UNION ALL
SELECT
  year,
  epci AS code,
  'epci' AS type,
  l_epci AS libelle,
  ST_Multi(ST_Union(geom_simple)) AS geom,
  ST_PointOnSurface(ST_Union(geom_simple)) AS centroid
FROM {{ ref('perimeters') }}
WHERE epci IS NOT NULL
GROUP BY year, epci, l_epci
UNION ALL
SELECT
  year,
  aom AS code,
  
  l_aom AS libelle,
  ST_Multi(ST_Union(geom_simple)) AS geom,
  ST_PointOnSurface(ST_Union(geom_simple)) AS centroid
FROM {{ ref('perimeters') }}
WHERE aom IS NOT NULL
GROUP BY year, aom, l_aom
UNION ALL
SELECT
  year,
  dep AS code,
  'dep' AS type,
  l_dep AS libelle,
  ST_Multi(ST_Union(geom_simple)) AS geom,
  ST_PointOnSurface(ST_Union(geom_simple)) AS centroid
FROM {{ ref('perimeters') }}
WHERE dep IS NOT NULL
GROUP BY year, dep, l_dep
UNION ALL
SELECT
  year,
  reg AS code,
  'reg' AS type,
  l_reg AS libelle,
  ST_Multi(ST_Union(geom_simple)) AS geom,
  ST_PointOnSurface(ST_Union(geom_simple)) AS centroid
FROM {{ ref('perimeters') }}
WHERE reg IS NOT NULL
GROUP BY year, reg, l_reg
UNION ALL
SELECT
  year,
  country AS code,
  'country' AS type,
  l_country AS libelle,
  ST_Multi(ST_Union(geom_simple)) AS geom,
  ST_PointOnSurface(ST_Union(geom_simple)) AS centroid
FROM {{ ref('perimeters') }}
WHERE country IS NOT NULL
GROUP BY year, country, l_country