{{ config(
  materialized='view',
  tags=['refined', 'direction', 'daily', 'year_com_to']
) }}

SELECT * FROM{{ ref('direction_year_arr_to') }}
UNION ALL
SELECT * FROM {{ ref('direction_year_plm_to') }}
