{#
  - role = 'driver' / 'passenger' : nouveau DANS ce rôle (un ancien conducteur qui
    devient passager compte comme nouveau passager, et inversement).
  - role = 'any'                  : nouveau tout court, sur sa toute première
    apparition dans le territoire tous rôles confondus (un ancien conducteur qui
    devient passager n'est pas recompté ici).
#}
{{ config(
  materialized='view',
  tags=['app_metabase', 'aom_monthly_acquisition']
) }}

WITH monthly AS (
  SELECT
    'aom'            AS perim,
    code,
    role,
    incremental_date AS date,
    active_users,
    new_users
  FROM {{ ref('user_aom_acquisition_month') }}
  UNION ALL
  SELECT
    'aomreg'         AS perim,
    code,
    role,
    incremental_date AS date,
    active_users,
    new_users
  FROM {{ ref('user_aomreg_acquisition_month') }}
)

SELECT
  perim,
  code,
  role,
  active_users,
  new_users,
  to_char(date, 'YYYY-MM') AS month,  -- noqa: RF04
  sum(new_users) OVER (
    PARTITION BY perim, code, role ORDER BY date
  )                        AS cumulative_users
FROM monthly
