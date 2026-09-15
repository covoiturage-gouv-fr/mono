{#
  Distribution du nombre de trajets mensuels par utilisateur, par aom
#}
{{ config(
  materialized='view',
  tags=['app_metabase', 'aom_monthly_user_carpools_distribution']
) }}

SELECT
  'aom'                                 AS perim,
  code,
  to_char(incremental_date, 'YYYY-MM')  AS date,
  role,
  users,
  q1_carpools,
  median_carpools,
  q3_carpools
FROM {{ ref('user_aom_carpools_distribution_month') }}
UNION ALL
SELECT
  'aomreg'                              AS perim,
  code,
  to_char(incremental_date, 'YYYY-MM')  AS date,
  role,
  users,
  q1_carpools,
  median_carpools,
  q3_carpools
FROM {{ ref('user_aomreg_carpools_distribution_month') }}
ORDER BY date, perim, code, role
