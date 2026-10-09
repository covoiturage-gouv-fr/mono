{#
  Distribution du nombre de trajets mensuels par utilisateur, par aom
#}
{{ config(
  materialized='view',
  tags=['app_metabase', 'aom_monthly_user_carpools_distribution']
) }}

{{ mb_union_aom_aomreg('carpools_distribution_month', [
  'code',
  "to_char(incremental_date, 'YYYY-MM') AS date",
  'role',
  'users',
  'q1_carpools',
  'median_carpools',
  'q3_carpools'
]) }}
