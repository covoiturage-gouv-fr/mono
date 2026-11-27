{#
  - role = 'driver' / 'passenger' : nouveau DANS ce rôle (un ancien conducteur qui
    devient passager compte comme nouveau passager, et inversement).
  - role = 'any'                  : nouveau tout court, sur sa toute première
    apparition dans le territoire tous rôles confondus.
#}
{{ config(
  materialized='view',
  tags=['app_metabase', 'territory_monthly_acquisition']
) }}

WITH monthly AS (
  {{ mb_union_territory('acquisition_month', [
    'code',
    'role',
    'incremental_date AS date',
    'active_users',
    'new_users'
  ]) }}
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
