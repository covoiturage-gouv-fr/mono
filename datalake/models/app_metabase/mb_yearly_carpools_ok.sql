{{ config(
  materialized='view',
  tags=['app_metabase', 'yearly_carpools_ok']
) }}

SELECT
  year,
  SUM(carpools_valid) AS count
FROM {{ ref('fraud_year_country_from') }}
GROUP BY year
ORDER BY year
