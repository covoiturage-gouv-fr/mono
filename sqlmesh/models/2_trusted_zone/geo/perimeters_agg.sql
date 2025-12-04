MODEL (
  name trusted_zone.perimeters_agg,
  kind FULL,
  start '2020-01-01',
  cron '@yearly',
  grain ['code','type','year'],
  tags ['trusted','geo', 'perimeters_agg']
);

SELECT
  year,
  arr AS code,
  'com' AS type,
  l_arr       AS libelle,
  geom_simple AS geom,
  geom_centroid AS centroid
FROM trusted_zone.perimeters
UNION ALL
SELECT
  year,
  epci AS code,
  'epci' AS type,
  l_epci AS libelle,
  ST_Multi(ST_Union(geom_simple)) AS geom,
  ST_PointOnSurface(ST_Union(geom_simple)) AS centroid
FROM trusted_zone.perimeters
WHERE epci IS NOT NULL
GROUP BY year, epci
UNION ALL
SELECT
  year,
  aom AS code,
  'aom' AS type,
  l_aom AS libelle,
  ST_Multi(ST_Union(geom_simple)) AS geom,
  ST_PointOnSurface(ST_Union(geom_simple)) AS centroid
FROM trusted_zone.perimeters
WHERE aom IS NOT NULL
GROUP BY year, aom
UNION ALL
SELECT
  year,
  dep AS code,
  'dep' AS type,
  l_dep AS libelle,
  ST_Multi(ST_Union(geom_simple)) AS geom,
  ST_PointOnSurface(ST_Union(geom_simple)) AS centroid
FROM trusted_zone.perimeters
WHERE dep IS NOT NULL
GROUP BY year, dep
UNION ALL
SELECT
  year,
  reg AS code,
  'reg' AS type,
  l_reg AS libelle,
  ST_Multi(ST_Union(geom_simple)) AS geom,
  ST_PointOnSurface(ST_Union(geom_simple)) AS centroid
FROM trusted_zone.perimeters
WHERE reg IS NOT NULL
GROUP BY year, reg
UNION ALL
SELECT
  year,
  country AS code,
  'country' AS type,
  ST_Multi(ST_Union(geom_simple)) AS geom,
  ST_PointOnSurface(ST_Union(geom_simple)) AS centroid
FROM trusted_zone.perimeters
WHERE country IS NOT NULL
GROUP BY year, country;
