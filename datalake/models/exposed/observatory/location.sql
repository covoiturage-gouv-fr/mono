{{ config(
    materialized='view',
    tags=['exposed', 'observatory', 'location']
) }}

-- Remplace la vue legacy `observatoire_stats.view_location`.
-- Sert la heatmap de densité (H3) de l'observatoire. Le binning se fait côté
-- api-datalake en SQL via `h3_cell_to_parent(<h3index_z8>, n)` (zoom n dans
-- [0,8]), d'où l'exposition directe des index H3 résolution 8 plutôt que des
-- lat/lon : parité H3 exacte pour n <= 8, sans rapatrier les positions.

SELECT
  _id               AS carpool_id,
  start_geo_code,
  end_geo_code,
  start_datetime_tz AS start_datetime,
  start_h3index_z8,
  end_h3index_z8
FROM {{ ref('carpools') }}
WHERE
  valid_acquisition_status
  AND start_datetime_tz >= '2020-01-01'
  AND start_datetime_tz < now() - INTERVAL '2 day'
