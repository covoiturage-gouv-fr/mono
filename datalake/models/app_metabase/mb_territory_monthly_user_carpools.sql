{{ config(
  materialized='view',
  tags=['app_metabase', 'territory_monthly_user_carpools']
) }}

{{ mb_union_territory('month', [
  'code',
  "to_char(incremental_date, 'YYYY-MM') AS date",
  'role',
  'user_id',
  'carpools'
]) }}
