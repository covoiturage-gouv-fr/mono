{#
  Grain brut mensuel par utilisateur : nombre de trajets par (utilisateur,
  territoire, role, mois).
#}
{{ config(
  materialized='view',
  tags=['app_metabase', 'aom_monthly_user_carpools']
) }}

{{ mb_union_aom_aomreg('month', [
  'code',
  "to_char(incremental_date, 'YYYY-MM') AS date",
  'role',
  'user_id',
  'carpools'
]) }}
