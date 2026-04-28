{{ config(
  materialized='view',
  tags=['refined', 'direction', 'daily', 'year_com_both']
) }}

SELECT * FROM {{ ref('direction_year_arr_both') }}
UNION ALL
SELECT * FROM {{ ref('direction_year_plm_both') }}
