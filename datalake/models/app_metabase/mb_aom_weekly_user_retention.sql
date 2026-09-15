{#
  Cohortes hebdo pour l'attrition depuis l'acquisition, par aom.
#}
{{ config(
  materialized='view',
  tags=['app_metabase', 'aom_weekly_user_retention']
) }}

WITH daily AS (
  SELECT 'aom' AS perim, user_id, role, code, incremental_date
  FROM {{ ref('user_aom_day') }}
  UNION ALL
  SELECT 'aomreg' AS perim, user_id, role, code, incremental_date
  FROM {{ ref('user_aomreg_day') }}
)

SELECT
  perim,
  code,
  role,
  user_id,
  MIN(date_trunc('week', incremental_date))::date AS first_week
FROM daily
GROUP BY perim, code, role, user_id
ORDER BY perim, code, role, user_id
