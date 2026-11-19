{{ config(
  materialized='incremental',
  unique_key=['dataset', 'h3_1', 'h3_2'],
  tags=['aggregated'],
  indexes=[
    { 'columns': ['dataset', 'h3_1', 'h3_2'], 'unique': true },
  ]
) }}

WITH pairs AS (
  SELECT DISTINCT
    o.h3_1,
    o.h3_2,
    o.lng_1,
    o.lat_1,
    o.lng_2,
    o.lat_2
  FROM {{ ref('h3_od_month') }} AS o
  {% if is_incremental() %}
    WHERE NOT EXISTS (
      SELECT 1 FROM {{ this }} AS t
      WHERE
        t.dataset = '{{ var("osrm_dataset") }}'
        AND o.h3_1 = t.h3_1 AND o.h3_2 = t.h3_2
    )
  {% endif %}
),

route_nodes AS (
  SELECT
    r.dataset,
    p.h3_1,
    p.h3_2,
    p.lng_1,
    p.lat_1,
    p.lng_2,
    p.lat_2,
    n.idx,
    c.h3
  FROM pairs AS p
  INNER JOIN {{ source('raw', 'osrm_route') }} AS r
    ON
      r.dataset = '{{ var("osrm_dataset") }}'
      AND r.status = 'ok'
      AND p.lng_1 = r.lng_1 AND p.lat_1 = r.lat_1
      AND p.lng_2 = r.lng_2 AND p.lat_2 = r.lat_2
  CROSS JOIN LATERAL unnest(r.nodes) WITH ORDINALITY AS n (node_id, idx)
  LEFT JOIN {{ ref('osrm_node_h3') }} AS c
    ON r.dataset = c.dataset AND n.node_id = c.node_id
),

bounds AS (
  SELECT
    dataset,
    h3_1,
    h3_2,
    lng_1,
    lat_1,
    lng_2,
    lat_2,
    count(*)                                         AS n_nodes,
    min(idx) FILTER (WHERE h3 IS DISTINCT FROM h3_1) AS first_out,
    max(idx) FILTER (WHERE h3 IS DISTINCT FROM h3_2) AS last_out
  FROM route_nodes
  GROUP BY dataset, h3_1, h3_2, lng_1, lat_1, lng_2, lat_2
)

-- On garde aussi le nœud d'avant la sortie de la cellule de départ et celui
-- d'après l'entrée dans la cellule d'arrivée : les tronçons qui franchissent
-- le bord restent sur la carte.
SELECT
  dataset,
  h3_1,
  h3_2,
  lng_1,
  lat_1,
  lng_2,
  lat_2,
  greatest(first_out - 1, 1)   AS first_idx,
  least(last_out + 1, n_nodes) AS last_idx
FROM bounds
WHERE
  -- greatest/least ignorent les NULL : une route qui ne quitte jamais la
  -- cellule de départ (ou n'arrive jamais hors de celle d'arrivée) est écartée
  first_out IS NOT NULL
  AND last_out IS NOT NULL
  AND least(last_out + 1, n_nodes) > greatest(first_out - 1, 1)
