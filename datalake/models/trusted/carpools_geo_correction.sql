{{
  config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='carpool_id',
    indexes=[
      { 'columns': ['carpool_id'], 'unique': true }
    ],
    tags=['trusted', 'carpools', 'daily']
  )
}}

WITH source_carpools AS (
  SELECT
    c._id,
    c.start_datetime,
    c.start_position::geometry AS start_position,
    c.end_position::geometry   AS end_position,
    g.start_geo_code,
    g.end_geo_code,
    g.updated_at               AS geo_updated_at
  FROM {{ source('dlk_import', 'carpool_v2_carpools') }} AS c
  INNER JOIN {{ source('dlk_import', 'carpool_v2_geo') }} AS g ON c._id = g.carpool_id
  WHERE {{ time_filter('c.start_datetime', 'start_datetime', lookback_nb=3) }}
),

-- communes rétablies (mod=21) — sous-ensemble rare de com_evolution
perimeters_retablissement AS (
  SELECT
    year,
    com,
    geom
  FROM {{ ref('perimeters') }} AS p
  WHERE com IN (
    SELECT DISTINCT ce.new_com
    FROM {{ ref('com_evolution') }} AS ce
    WHERE
      ce.mod = 21
      AND ce.year = p.year
  )
),

-- com_evolution est borné par la source insee_mvt_com_2025 : son année max
-- sert à rabattre les trajets géocodés une année plus récente (ex. 2026) sur
-- les dernières évolutions connues, sinon ils ne seraient jamais corrigés.
max_evolution_year AS (
  SELECT MAX(year) AS y FROM {{ ref('com_evolution') }}
),

-- une seule cible par (old_com, année) : un old_com peut cumuler plusieurs
-- mouvements la même année (recodage + fusion), ce qui dédoublerait les
-- carpool_id en sortie et violerait l'index unique. On en garde un seul.
com_recodage AS (
  SELECT DISTINCT ON (old_com, year)
    old_com,
    new_com,
    year
  FROM {{ ref('com_evolution') }}
  WHERE mod IN (31, 32, 33, 41, 50)
  ORDER BY old_com, year, mod, new_com
),

-- carpools dont au moins un bout est dans une commune rétablie (mod=21)
affected_retablissement AS (
  SELECT DISTINCT sc._id
  FROM source_carpools AS sc
  CROSS JOIN max_evolution_year AS mey
  INNER JOIN {{ ref('com_evolution') }} AS ce
    ON
      (sc.start_geo_code = ce.old_com OR sc.end_geo_code = ce.old_com)
      AND ce.mod = 21
      AND ce.year <= LEAST(EXTRACT(YEAR FROM sc.geo_updated_at)::int, mey.y)
),

-- carpools dont au moins un bout est dans une commune fusionnée ou recodée
-- (mod=31,32,33,41,50)
affected_fusion AS (
  SELECT DISTINCT sc._id
  FROM source_carpools AS sc
  CROSS JOIN max_evolution_year AS mey
  INNER JOIN com_recodage AS ce
    ON
      (sc.start_geo_code = ce.old_com OR sc.end_geo_code = ce.old_com)
      AND ce.year <= LEAST(EXTRACT(YEAR FROM sc.geo_updated_at)::int, mey.y)
),

affected AS (
  SELECT _id FROM affected_retablissement
  UNION
  SELECT _id FROM affected_fusion
)

SELECT
  sc._id                                          AS carpool_id,
  sc.start_datetime,
  COALESCE(ps.com, cs.new_com, sc.start_geo_code) AS start_geo_code,
  COALESCE(pe.com, ce.new_com, sc.end_geo_code)   AS end_geo_code
FROM source_carpools AS sc
CROSS JOIN max_evolution_year AS mey
INNER JOIN affected AS a ON sc._id = a._id
LEFT JOIN LATERAL (
  SELECT p.com
  FROM perimeters_retablissement AS p
  WHERE
    ST_INTERSECTS(sc.start_position, p.geom)
    AND p.year <= LEAST(EXTRACT(YEAR FROM sc.geo_updated_at)::int, mey.y)
  ORDER BY p.year DESC
  LIMIT 1
) AS ps ON TRUE
LEFT JOIN LATERAL (
  SELECT p.com
  FROM perimeters_retablissement AS p
  WHERE
    ST_INTERSECTS(sc.end_position, p.geom)
    AND p.year <= LEAST(EXTRACT(YEAR FROM sc.geo_updated_at)::int, mey.y)
  ORDER BY p.year DESC
  LIMIT 1
) AS pe ON TRUE
LEFT JOIN LATERAL (
  SELECT cr.new_com
  FROM com_recodage AS cr
  WHERE
    cr.old_com = sc.start_geo_code
    AND cr.year <= LEAST(EXTRACT(YEAR FROM sc.geo_updated_at)::int, mey.y)
  ORDER BY cr.year DESC
  LIMIT 1
) AS cs ON TRUE
LEFT JOIN LATERAL (
  SELECT cr.new_com
  FROM com_recodage AS cr
  WHERE
    cr.old_com = sc.end_geo_code
    AND cr.year <= LEAST(EXTRACT(YEAR FROM sc.geo_updated_at)::int, mey.y)
  ORDER BY cr.year DESC
  LIMIT 1
) AS ce ON TRUE
