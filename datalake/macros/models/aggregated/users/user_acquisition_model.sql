{% macro user_acquisition_model(perim) %}

  {% if perim not in ['aom', 'aomreg'] %}
    {{ exceptions.raise_compiler_error("Invalid perim: " ~ perim ~ ". Expected: aom, aomreg") }}
  {% endif %}

{# Table fulle refresh a-la users.sql afin de détecter correctement les nouveaux users#}
{{ config(
  materialized='table',
  indexes=[
    {'columns': ['code', 'role', 'incremental_date'], 'unique': true}
  ],
  tags=['aggregated', 'users', 'month', perim, 'acquisition', 'daily']
) }}

WITH monthly AS (
  SELECT code, role, user_id, incremental_date
  FROM {{ ref('user_' ~ perim ~ '_month') }}
),

first_month_by_role AS (
  SELECT code, role, user_id, MIN(incremental_date) AS first_date
  FROM monthly
  GROUP BY code, role, user_id
),

by_month_role AS (
  SELECT code, role, incremental_date AS date, COUNT(DISTINCT user_id) AS active_users
  FROM monthly
  GROUP BY code, role, incremental_date
),

new_by_month_role AS (
  SELECT code, role, first_date AS date, COUNT(*) AS new_users
  FROM first_month_by_role
  GROUP BY code, role, first_date
),

first_month_any AS (
  SELECT code, user_id, MIN(incremental_date) AS first_date
  FROM monthly
  GROUP BY code, user_id
),

by_month_any AS (
  SELECT code, incremental_date AS date, COUNT(DISTINCT user_id) AS active_users
  FROM monthly
  GROUP BY code, incremental_date
),

new_by_month_any AS (
  SELECT code, first_date AS date, COUNT(*) AS new_users
  FROM first_month_any
  GROUP BY code, first_date
)

SELECT
  b.code,
  b.role,  -- noqa: RF04
  b.date AS incremental_date,
  b.active_users,
  COALESCE(n.new_users, 0) AS new_users
FROM by_month_role b
LEFT JOIN new_by_month_role n
  ON n.code = b.code
  AND n.role = b.role
  AND n.date = b.date

UNION ALL

SELECT
  b.code,
  'any' AS role,  -- noqa: RF04
  b.date AS incremental_date,
  b.active_users,
  COALESCE(n.new_users, 0) AS new_users
FROM by_month_any b
LEFT JOIN new_by_month_any n
  ON n.code = b.code
  AND n.date = b.date

{% endmacro %}
