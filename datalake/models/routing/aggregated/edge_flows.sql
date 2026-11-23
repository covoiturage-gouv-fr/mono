{{ config(
  materialized='incremental',
  incremental_strategy='append',
  tags=['aggregated'],
  pre_hook="""
    {% if is_incremental() %}
      DELETE FROM {{ this }} AS f
      USING ({{ routing_periods() }}) AS p
      WHERE f.year = p.year AND f.month IS NOT DISTINCT FROM p.month
    {% endif %}
  """,
  post_hook="""
    CREATE UNIQUE INDEX IF NOT EXISTS edge_flows_period_edge_uidx
    ON {{ this }} (year, month, node_a, node_b) NULLS NOT DISTINCT
  """
) }}

-- Flux par tronçon de route et par période (direction both) : chaque flux
-- entre cellules H3 z8 est reporté sur les tronçons de sa route coupée
-- (route_trim). Un tronçon = deux nœuds OSM consécutifs ; les deux sens d'une
-- même voie sont additionnés sous node_a < node_b. pairs et top_pair_journeys
-- (paires de cellules non orientées) servent au seuil d'anonymisation.
-- Périodes : routing_periods() (month NULL = année complète). Un passage
-- remplace les périodes de sa fenêtre et garde les autres.
-- Géométrie : edge_geom.
WITH periods AS (
  {{ routing_periods() }}
),

-- Les routes ne sont découpées en tronçons qu'au mois ; les années sont la
-- somme de leurs mois (per_pair), sans second découpage.
od AS (
  SELECT
    o.year,
    o.month,
    o.h3_1,
    o.h3_2,
    least(o.h3_1, o.h3_2)    AS cell_a,
    greatest(o.h3_1, o.h3_2) AS cell_b,
    sum(o.journeys)          AS journeys
  FROM {{ ref('h3_od_month') }} AS o
  INNER JOIN periods AS p
    ON o.year = p.year AND o.month = p.month
  GROUP BY o.year, o.month, o.h3_1, o.h3_2
),

edges AS (
  SELECT
    od.year,
    od.month,
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

per_pair_month AS (
  SELECT
    year,
    month,
    node_a,
    node_b,
    cell_a,
    cell_b,
    sum(journeys) AS journeys
  FROM edges
  GROUP BY year, month, node_a, node_b, cell_a, cell_b
),

per_pair AS (
  SELECT * FROM per_pair_month
  UNION ALL
  SELECT
    m.year,
    NULL::int       AS month,  -- noqa: RF04
    m.node_a,
    m.node_b,
    m.cell_a,
    m.cell_b,
    sum(m.journeys) AS journeys
  FROM per_pair_month AS m
  INNER JOIN periods AS p ON m.year = p.year AND p.month IS NULL
  GROUP BY m.year, m.node_a, m.node_b, m.cell_a, m.cell_b
)

SELECT
  year,
  month,
  node_a,
  node_b,
  sum(journeys) AS journeys,
  count(*)      AS pairs,
  max(journeys) AS top_pair_journeys
FROM per_pair
GROUP BY year, month, node_a, node_b
