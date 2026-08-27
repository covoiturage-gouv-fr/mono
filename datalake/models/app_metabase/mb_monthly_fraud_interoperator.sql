{{ config(
  materialized='view',
  tags=['app_metabase', 'monthly_fraud_interoperator']
) }}

SELECT
  operator_name,
  incremental_date        AS mois,
  SUM(carpools_fraud)     AS operator_journey_id_count,
  SUM(avoided_incentives) AS avoided_incentives
FROM {{ ref('fraud_month_country_from') }}
WHERE incremental_date > '2023-06-01'
GROUP BY operator_name, incremental_date
HAVING SUM(carpools_fraud) > 0
ORDER BY incremental_date
