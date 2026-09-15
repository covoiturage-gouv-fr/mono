{#
  Cohortes hebdo pour l'attrition depuis l'acquisition, par aom.
  - role = 'driver' / 'passenger' : 1re semaine dans CE rôle (un ancien conducteur
    qui devient passager a sa propre first_week côté passager).
  - role = 'any'                  : 1re semaine tout court, tous rôles confondus.
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

UNION ALL

SELECT
  perim,
  code,
  'any' AS role,  -- noqa: RF04
  user_id,
  MIN(date_trunc('week', incremental_date))::date AS first_week
FROM daily
GROUP BY perim, code, user_id

ORDER BY perim, code, role, user_id
