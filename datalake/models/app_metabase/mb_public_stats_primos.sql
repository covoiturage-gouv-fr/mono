{{ config(
  materialized='view',
  tags=['app_metabase', 'public_stats_primos']
) }}


select
  count(*) filter (where first_date_driver is not null)    as primo_drivers,
  count(*) filter (where first_date_passenger is not null) as primo_passengers
from {{ ref('users') }}
