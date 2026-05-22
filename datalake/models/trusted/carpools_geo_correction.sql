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
    g.updated_at AS geo_updated_at
  FROM {{ source('carpool_v2', 'carpools') }} c
  JOIN {{ source('carpool_v2', 'geo') }} g ON g.carpool_id = c._id
  WHERE {{ time_filter('c.start_datetime', 'start_datetime', lookback_nb=3) }}
),

-- communes rétablies (mod=21) — sous-ensemble rare de com_evolution
perimeters_retablissement AS (
  SELECT year, com, geom
  FROM {{ ref('perimeters') }} p
  WHERE com IN (
    SELECT DISTINCT new_com
    FROM {{ ref('com_evolution') }}
    WHERE mod = 21
      AND year = p.year
  )
),

-- carpools dont au moins un bout est dans une commune rétablie (mod=21)
affected_retablissement AS (
  SELECT DISTINCT sc._id
  FROM source_carpools sc
  JOIN {{ ref('com_evolution') }} ce
    ON (sc.start_geo_code = ce.old_com OR sc.end_geo_code = ce.old_com)
    AND ce.mod = 21
    AND ce.year = EXTRACT(YEAR FROM sc.geo_updated_at)::int
),

-- carpools dont au moins un bout est dans une commune fusionnée (mod=32)
affected_fusion AS (
  SELECT DISTINCT sc._id
  FROM source_carpools sc
  JOIN {{ ref('com_evolution') }} ce
    ON (sc.start_geo_code = ce.old_com OR sc.end_geo_code = ce.old_com)
    AND ce.mod = 32
    AND ce.year = EXTRACT(YEAR FROM sc.geo_updated_at)::int
),

affected AS (
  SELECT _id FROM affected_retablissement
  UNION
  SELECT _id FROM affected_fusion
)

SELECT
  sc._id            AS carpool_id,
  sc.start_datetime,
  COALESCE(ps.com, cs.new_com, sc.start_geo_code) AS start_geo_code,
  COALESCE(pe.com, ce.new_com, sc.end_geo_code)   AS end_geo_code
FROM source_carpools sc
JOIN affected a ON a._id = sc._id
LEFT JOIN perimeters_retablissement ps
  ON ST_Intersects(sc.start_position, ps.geom)
  AND ps.year = EXTRACT(YEAR FROM sc.geo_updated_at)::int
LEFT JOIN perimeters_retablissement pe
  ON ST_Intersects(sc.end_position, pe.geom)
  AND pe.year = EXTRACT(YEAR FROM sc.geo_updated_at)::int
LEFT JOIN {{ ref('com_evolution') }} cs
  ON sc.start_geo_code = cs.old_com
  AND cs.mod = 32
  AND cs.year = EXTRACT(YEAR FROM sc.geo_updated_at)::int
LEFT JOIN {{ ref('com_evolution') }} ce
  ON sc.end_geo_code = ce.old_com
  AND ce.mod = 32
  AND ce.year = EXTRACT(YEAR FROM sc.geo_updated_at)::int
