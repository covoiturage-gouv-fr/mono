{% macro user_carpools_distribution_model(perim) %}

  {% if perim not in ['aom', 'aomreg'] %}
    {{ exceptions.raise_compiler_error("Invalid perim: " ~ perim ~ ". Expected: aom, aomreg") }}
  {% endif %}

{{ config(
  materialized='incremental',
  incremental_strategy='delete+insert',
  unique_key=['code', 'role', 'incremental_date'],
  indexes=[
    {'columns': ['code', 'role', 'incremental_date'], 'unique': true}
  ],
  tags=['aggregated', 'users', 'month', perim, 'carpools_distribution', 'daily']
) }}

{# Distribution du nombre de trajets DANS le mois (engagement du mois).
   role = 'any' : trajets tous rôles confondus #}
WITH monthly AS (
  SELECT code, role, user_id, incremental_date, carpools
  FROM {{ ref('user_' ~ perim ~ '_month') }}
  WHERE {{ time_filter(
    'incremental_date', type='date', default_start="'2020-01-01'",
    lookback_nb=1, lookback_unit='month'
  ) }}
),

any_role AS (
  SELECT
    code,
    'any' AS role,  -- noqa: RF04
    incremental_date,
    SUM(carpools) AS carpools
  FROM monthly
  GROUP BY code, user_id, incremental_date
),

combined AS (
  SELECT code, role, incremental_date, carpools FROM monthly
  UNION ALL
  SELECT code, role, incremental_date, carpools FROM any_role
)

SELECT
  code,
  role,  -- noqa: RF04
  incremental_date,
  COUNT(*) AS users,
  percentile_cont(0.25) WITHIN GROUP (ORDER BY carpools)
    AS q1_carpools,
  percentile_cont(0.5) WITHIN GROUP (ORDER BY carpools)
    AS median_carpools,
  percentile_cont(0.75) WITHIN GROUP (ORDER BY carpools)
    AS q3_carpools
FROM combined
GROUP BY code, role, incremental_date

{% endmacro %}
