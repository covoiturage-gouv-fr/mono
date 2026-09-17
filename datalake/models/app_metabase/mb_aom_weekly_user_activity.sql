{#
  Grain hebdo de l'activité utilisateur par aom. Sert, avec
  mb_aom_weekly_user_retention (first_week), au calcul de la courbe de rétention à
  n'importe quelle date côté question Metabase.
  - role = 'driver' / 'passenger' : actif CE rôle-là cette semaine.
  - role = 'any'                  : actif cette semaine-là, tous rôles confondus.
#}
{{ config(
  materialized='view',
  tags=['app_metabase', 'aom_weekly_user_activity']
) }}

WITH daily AS (
  SELECT
    'aom' AS perim,
    user_id,
    role,
    code,
    incremental_date
  FROM {{ ref('user_aom_day') }}
  UNION ALL
  SELECT
    'aomreg' AS perim,
    user_id,
    role,
    code,
    incremental_date
  FROM {{ ref('user_aomreg_day') }}
)

SELECT DISTINCT
  perim,
  code,
  role,
  user_id,
  date_trunc('week', incremental_date)::date AS week
FROM daily

UNION ALL

SELECT DISTINCT
  perim,
  code,
  'any'                                      AS role,  -- noqa: RF04
  user_id,
  date_trunc('week', incremental_date)::date AS week
FROM daily
