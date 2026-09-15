{#
  Nouveaux utilisateurs par mois et cumul, par aom.
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
  SELECT 'aom' AS perim, user_id, role, code, incremental_date
  FROM {{ ref('user_aom_month') }}
  UNION ALL
  SELECT 'aomreg' AS perim, user_id, role, code, incremental_date
  FROM {{ ref('user_aomreg_month') }}
),

first_month_by_role AS (
  SELECT
    perim,
    code,
    role,
    user_id,
    MIN(incremental_date) AS first_date
  FROM monthly
  GROUP BY perim, code, role, user_id
),

by_month_role AS (
  SELECT
    perim,
    code,
    role,
    incremental_date        AS date,
    COUNT(DISTINCT user_id) AS active_users
  FROM monthly
  GROUP BY perim, code, role, incremental_date
),

new_by_month_role AS (
  SELECT
    perim,
    code,
    role,
    first_date AS date,
    COUNT(*)   AS new_users
  FROM first_month_by_role
  GROUP BY perim, code, role, first_date
),

first_month_any AS (
  SELECT
    perim,
    code,
    user_id,
    MIN(incremental_date) AS first_date
  FROM monthly
  GROUP BY perim, code, user_id
),

by_month_any AS (
  SELECT
    perim,
    code,
    incremental_date        AS date,
    COUNT(DISTINCT user_id) AS active_users
  FROM monthly
  GROUP BY perim, code, incremental_date
),

new_by_month_any AS (
  SELECT
    perim,
    code,
    first_date AS date,
    COUNT(*)   AS new_users
  FROM first_month_any
  GROUP BY perim, code, first_date
),

combined AS (
  SELECT
    b.perim,
    b.code,
    b.role,
    b.date,
    b.active_users,
    COALESCE(n.new_users, 0) AS new_users
  FROM by_month_role b
  LEFT JOIN new_by_month_role n
    ON n.perim = b.perim
    AND n.code = b.code
    AND n.role = b.role
    AND n.date = b.date

  UNION ALL

  SELECT
    b.perim,
    b.code,
    'any' AS role,
    b.date,
    b.active_users,
    COALESCE(n.new_users, 0) AS new_users
  FROM by_month_any b
  LEFT JOIN new_by_month_any n
    ON n.perim = b.perim
    AND n.code = b.code
    AND n.date = b.date
)

SELECT
  perim,
  code,
  to_char(date, 'YYYY-MM') AS month,
  role,
  active_users,
  new_users,
  SUM(new_users) OVER (
    PARTITION BY perim, code, role ORDER BY date
  ) AS cumulative_users
FROM combined
ORDER BY month, perim, code, role
