{{ config(
  materialized='view',
  tags=['app_metabase', 'monthly_carpools_ok']
) }}

SELECT
  incremental_date    AS mois,
  SUM(carpools_valid) AS count
FROM {{ ref('fraud_month_country_from') }}
GROUP BY incremental_date
ORDER BY incremental_date
