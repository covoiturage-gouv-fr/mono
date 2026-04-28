{{ config(
  materialized='view',
  tags=['refined', 'direction', 'daily', 'year_com_from']
) }}

SELECT * FROM{{ ref('direction_year_arr_from') }}
UNION ALL
SELECT * FROM {{ ref('direction_year_plm_from') }}
