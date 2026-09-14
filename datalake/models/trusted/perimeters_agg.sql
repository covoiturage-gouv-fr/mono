{{ config(
  materialized='table',
  tags=['trusted', 'geo', 'perimeters_agg'],
  indexes=[
    { 'columns': ['code', 'type', 'year'], 'unique': true },
    { 'columns': ['centroid'], 'type': 'gist' },
    { 'columns': ['geom'], 'type': 'gist' },
  ]
) }}

SELECT
  year,
  arr                   AS code,
  'com'                 AS type,  -- noqa: RF04
  l_arr                 AS libelle,
  geom_simple           AS geom,
  -- le centroïde communal source est un MULTIPOINT ; on le réduit à un POINT
  -- unique pour que st_x/st_y (exposed observatory od_*) fonctionnent (ST_X
  -- exige un POINT)
  ST_CENTROID(centroid) AS centroid
FROM {{ ref('perimeters') }}
WHERE com IS NOT NULL
UNION ALL
SELECT
  year,
  epci                                     AS code,
  'epci'                                   AS type,  -- noqa: RF04
  l_epci                                   AS libelle,
  ST_MULTI(ST_UNION(geom_simple))          AS geom,
  ST_POINTONSURFACE(ST_UNION(geom_simple)) AS centroid
FROM {{ ref('perimeters') }}
WHERE epci IS NOT NULL
GROUP BY year, epci, l_epci
UNION ALL
SELECT
  year,
  aom                                      AS code,
  'aom'                                    AS type,  -- noqa: RF04
  MODE() WITHIN GROUP (ORDER BY l_aom)     AS libelle,
  ST_MULTI(ST_UNION(geom_simple))          AS geom,
  ST_POINTONSURFACE(ST_UNION(geom_simple)) AS centroid
FROM {{ ref('perimeters') }}
WHERE aom IS NOT NULL
GROUP BY year, aom
UNION ALL
SELECT
  year,
  dep                                      AS code,
  'dep'                                    AS type,  -- noqa: RF04
  l_dep                                    AS libelle,
  ST_MULTI(ST_UNION(geom_simple))          AS geom,
  ST_POINTONSURFACE(ST_UNION(geom_simple)) AS centroid
FROM {{ ref('perimeters') }}
WHERE dep IS NOT NULL
GROUP BY year, dep, l_dep
UNION ALL
SELECT
  year,
  reg                                      AS code,
  'reg'                                    AS type,  -- noqa: RF04
  l_reg                                    AS libelle,
  ST_MULTI(ST_UNION(geom_simple))          AS geom,
  ST_POINTONSURFACE(ST_UNION(geom_simple)) AS centroid
FROM {{ ref('perimeters') }}
WHERE reg IS NOT NULL
GROUP BY year, reg, l_reg
UNION ALL
SELECT
  year,
  country                                  AS code,
  'country'                                AS type,  -- noqa: RF04
  MODE() WITHIN GROUP (ORDER BY l_country) AS libelle,
  ST_MULTI(ST_UNION(geom_simple))          AS geom,
  ST_POINTONSURFACE(ST_UNION(geom_simple)) AS centroid
FROM {{ ref('perimeters') }}
WHERE country IS NOT NULL
GROUP BY year, country
UNION ALL
SELECT
  year,
  country                                  AS code,
  'com'                                    AS type,  -- noqa: RF04
  MODE() WITHIN GROUP (ORDER BY l_country) AS libelle,
  ST_MULTI(ST_UNION(geom_simple))          AS geom,
  ST_POINTONSURFACE(ST_UNION(geom_simple)) AS centroid
FROM {{ ref('perimeters') }}
WHERE country IS NOT NULL
GROUP BY year, country
UNION ALL
-- Territoires custom : union de communes résolue par `just custom-
-- territories-compile` (seeds custom_territories / _meta). Slug non
-- numérique => pas de collision avec les codes SIREN/INSEE des autres types.
-- Rattachés au dernier millésime disponible.
SELECT
  (SELECT MAX(py.year) FROM {{ ref('perimeters') }} AS py) AS year,
  ct.id                                                    AS code,
  'custom'                                                 AS type,
  m.libelle,
  ST_MULTI(ST_UNION(p.geom_simple))                        AS geom,
  ST_POINTONSURFACE(ST_UNION(p.geom_simple))               AS centroid
FROM {{ ref('custom_territories') }} AS ct
INNER JOIN {{ ref('custom_territories_meta') }} AS m
  ON ct.id = m.id AND m.active
INNER JOIN {{ ref('perimeters') }} AS p
  ON
    ct.arr = p.arr
    AND p.year = (SELECT MAX(py.year) FROM {{ ref('perimeters') }} AS py)
GROUP BY ct.id, m.libelle
