{#
  Composite réordonné, (start_code, end_code) en tête = clé du delete+insert. Sinon le
  planificateur choisit l'index ['start_code', 'end_code'] seul, peu sélectif (une paire
  origine-destination revient sur des milliers de lignes) : le DELETE incrémental balaie
  cet index et devient catastrophique (> 1 h par fenêtre). Avec (start_code, end_code) en
  tête, le composite sert le DELETE (sonde par ligne sur les 5 colonnes) ET les lookups par
  (start_code, end_code) via son préfixe gauche ; l'index ['start_code', 'end_code'] seul
  devient redondant et est retiré.
#}
{{
  config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key=['user_id', 'incremental_date', 'role', 'start_code', 'end_code'],
    indexes=[
      { 'columns': ['start_code', 'end_code', 'user_id', 'incremental_date', 'role'] },
      { 'columns': ['incremental_date'] }
    ],
    tags=['aggregated', 'users', 'od', 'daily']
  )
}}

WITH filtered_carpools AS (
  {{ filtered_carpools(
    'arr', lookback_nb=3, lookback_unit='day', with_new_users=false
  ) }}
),

carpools_by_role AS (
  SELECT
    driver_key     AS user_id,
    'driver'::text AS role,  -- noqa: RF04
    *
  FROM filtered_carpools
  WHERE driver_key IS NOT NULL
  UNION ALL
  SELECT
    passenger_key     AS user_id,
    'passenger'::text AS role,  -- noqa: RF04
    *
  FROM filtered_carpools
  WHERE passenger_key IS NOT NULL
)

SELECT
  user_id,
  role,  -- noqa: RF04
  carpool_datetime::date   AS incremental_date,
  start_code,
  end_code,
  {{ user_agg_columns() }}
FROM carpools_by_role
GROUP BY 1, 2, 3, 4, 5
