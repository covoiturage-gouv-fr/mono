{{ config(
  materialized='incremental',
  unique_key=['node_a', 'node_b'],
  tags=['aggregated'],
  indexes=[
    { 'columns': ['node_a', 'node_b'], 'unique': true },
  ]
) }}

-- Géométrie des tronçons chargés, une ligne par tronçon toutes périodes
-- confondues : edge_flows ne porte que les compteurs, pour ne pas dupliquer
-- la géométrie à chaque mois et chaque année. Seuls les nouveaux tronçons
-- sont ajoutés.
WITH new_edges AS (
  SELECT DISTINCT
    f.node_a,
    f.node_b
  FROM {{ ref('edge_flows') }} AS f
  {% if is_incremental() %}
    WHERE NOT EXISTS (
      SELECT 1 FROM {{ this }} AS g
      WHERE g.node_a = f.node_a AND g.node_b = f.node_b
    )
  {% endif %}
)

SELECT
  e.node_a,
  e.node_b,
  st_makeline(
    st_setsrid(st_makepoint(a.lng::float8, a.lat::float8), 4326),
    st_setsrid(st_makepoint(b.lng::float8, b.lat::float8), 4326)
  ) AS geom
FROM new_edges AS e
INNER JOIN {{ source('raw', 'osrm_node') }} AS a
  ON a.dataset = '{{ var("osrm_dataset") }}' AND e.node_a = a.node_id
INNER JOIN {{ source('raw', 'osrm_node') }} AS b
  ON b.dataset = '{{ var("osrm_dataset") }}' AND e.node_b = b.node_id
