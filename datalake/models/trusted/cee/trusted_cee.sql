{{ config(
  materialized='view',
  tags=['trusted', 'cee'],
) }}

SELECT * 
FROM {{ ref('raw_cee') }}
