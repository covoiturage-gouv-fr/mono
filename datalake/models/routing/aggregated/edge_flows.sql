{{ config(
  materialized='table',
  tags=['aggregated'],
  indexes=[
    {'columns': ['node_a', 'node_b'], 'unique': true},
  ]
) }}

-- Flux par tronçon de route sur toute la fenêtre routing_from / routing_to
-- (direction both) : chaque flux entre cellules H3 z8 est reporté sur les
-- tronçons de sa route coupée (route_trim). Un tronçon = deux nœuds OSM
-- consécutifs ; les deux sens d'une même voie sont additionnés sous
-- node_a < node_b. pairs et top_pair_journeys (paires de cellules non
-- orientées) servent au seuil d'anonymisation de la carte.
-- Table de travail des tuiles : elle se recalcule depuis le cache OSRM.
WITH od AS (
  SELECT
    h3_1,
    h3_2,
    least(h3_1, h3_2)    AS cell_a,
    greatest(h3_1, h3_2) AS cell_b,
    sum(journeys)        AS journeys
  FROM {{ ref('h3_od_month') }}
  GROUP BY h3_1, h3_2
),

edges AS (
  SELECT
    od.cell_a,
    od.cell_b,
    od.journeys,
    least(e.node_from, e.node_to)    AS node_a,
    greatest(e.node_from, e.node_to) AS node_b
  FROM od
  INNER JOIN {{ ref('route_trim') }} AS t
    ON
      t.dataset = '{{ var("osrm_dataset") }}'
      AND od.h3_1 = t.h3_1 AND od.h3_2 = t.h3_2
  INNER JOIN {{ source('raw', 'osrm_route') }} AS r
    ON
      t.dataset = r.dataset
      AND t.lng_1 = r.lng_1 AND t.lat_1 = r.lat_1
      AND t.lng_2 = r.lng_2 AND t.lat_2 = r.lat_2
  CROSS JOIN
    LATERAL (
      SELECT
        n.node_id                             AS node_from,
        lead(n.node_id) OVER (ORDER BY n.idx) AS node_to
      FROM
        unnest(r.nodes[t.first_idx:t.last_idx]) WITH ORDINALITY
          AS n (node_id, idx)
    ) AS e
  WHERE e.node_to IS NOT NULL
),

per_pair AS (
  SELECT
    node_a,
    node_b,
    cell_a,
    cell_b,
    sum(journeys) AS journeys
  FROM edges
  GROUP BY node_a, node_b, cell_a, cell_b
),

flows AS (
  SELECT
    node_a,
    node_b,
    sum(journeys) AS journeys,
    count(*)      AS pairs,
    max(journeys) AS top_pair_journeys
  FROM per_pair
  GROUP BY node_a, node_b
)

SELECT
  f.node_a,
  f.node_b,
  f.journeys,
  f.pairs,
  f.top_pair_journeys,
  st_makeline(
    st_setsrid(st_makepoint(a.lng::float8, a.lat::float8), 4326),
    st_setsrid(st_makepoint(b.lng::float8, b.lat::float8), 4326)
  ) AS geom
FROM flows AS f
INNER JOIN {{ source('raw', 'osrm_node') }} AS a
  ON a.dataset = '{{ var("osrm_dataset") }}' AND f.node_a = a.node_id
INNER JOIN {{ source('raw', 'osrm_node') }} AS b
  ON b.dataset = '{{ var("osrm_dataset") }}' AND f.node_b = b.node_id
