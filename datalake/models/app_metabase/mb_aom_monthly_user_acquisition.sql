{#
  Nouveaux utilisateurs par mois et cumul : un utilisateur est "nouveau" sur une aom
  le mois de sa première apparition dans user_aom_month/user_aomreg_month
#}
{{ config(
  materialized='view',
  tags=['app_metabase', 'aom_monthly_acquisition']
) }}

WITH monthly AS (
  SELECT 'aom' AS perim, user_id, role, code, incremental_date
  FROM {{ ref('user_aom_month') }}
  UNION ALL
  SELECT 'aomreg' AS perim, user_id, role, code, incremental_date
  FROM {{ ref('user_aomreg_month') }}
),

first_month AS (
  SELECT
    perim,
    code,
    role,
    user_id,
    MIN(incremental_date) AS first_date
  FROM monthly
  GROUP BY perim, code, role, user_id
),

by_month AS (
  SELECT
    perim,
    code,
    role,
    incremental_date        AS date,
    COUNT(DISTINCT user_id) AS active_users
  FROM monthly
  GROUP BY perim, code, role, incremental_date
),

new_by_month AS (
  SELECT
    perim,
    code,
    role,
    first_date AS date,
    COUNT(*)   AS new_users
  FROM first_month
  GROUP BY perim, code, role, first_date
),

combined AS (
  SELECT
    b.perim,
    b.code,
    b.role,
    b.date,
    b.active_users,
    COALESCE(n.new_users, 0) AS new_users
  FROM by_month b
  LEFT JOIN new_by_month n
    ON n.perim = b.perim
    AND n.code = b.code
    AND n.role = b.role
    AND n.date = b.date
)

SELECT
  perim,
  code,
  to_char(date, 'YYYY-MM') AS date,
  role,
  active_users,
  new_users,
  SUM(new_users) OVER (
    PARTITION BY perim, code, role ORDER BY date
  ) AS cumulative_users
FROM combined
ORDER BY date, perim, code, role
