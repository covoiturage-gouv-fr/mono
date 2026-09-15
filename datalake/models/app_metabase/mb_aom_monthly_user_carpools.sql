{#
  Grain brut mensuel par utilisateur : nombre de trajets par (utilisateur,
  territoire, role, mois).
#}
{{ config(
  materialized='view',
  tags=['app_metabase', 'aom_monthly_user_carpools']
) }}

SELECT
  'aom'                                 AS perim,
  code,
  to_char(incremental_date, 'YYYY-MM')  AS date,
  role,
  user_id,
  carpools
FROM {{ ref('user_aom_month') }}
UNION ALL
SELECT
  'aomreg'                              AS perim,
  code,
  to_char(incremental_date, 'YYYY-MM')  AS date,
  role,
  user_id,
  carpools
FROM {{ ref('user_aomreg_month') }}
ORDER BY date, perim, code, role
