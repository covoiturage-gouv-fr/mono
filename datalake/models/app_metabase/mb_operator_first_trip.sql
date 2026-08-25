{{ config(
  materialized='view',
  tags=['app_metabase', 'operator_first_trip']
) }}

select
  operator_id,
  operator_name,
  carpools,
  first_date as first_trip,
  last_date  as last_trip
from {{ ref('operators') }}
