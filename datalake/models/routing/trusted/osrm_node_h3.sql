{{ config(materialized='ephemeral', tags=['trusted']) }}

-- Éphémère : la cellule se recalcule à la volée plutôt que de dupliquer
-- zone_raw.osrm_node (0,7 Go en octobre 2026).
SELECT
  dataset,
  node_id,
  h3_lat_lng_to_cell(point(lng::float8, lat::float8), 8) AS h3
FROM {{ source('raw', 'osrm_node') }}
WHERE dataset = '{{ var("osrm_dataset") }}'
