{{ config(
  materialized='view',
  tags=['app_metabase', 'public_stats']
) }}

with fraudulous_carpools as (
  select SUM(carpools_fraud) as fraudulous_carpools_count
  from {{ ref('fraud_year_country_from') }}
),

valid_carpools as (
  select
    TRUNC((SUM(passenger_seats)::numeric / SUM(trips)::numeric) + 1, 2)
      as average_carpoolers_by_car,
    COUNT(distinct operator_id)
      as operators_count,
    SUM(carpools)
      as validated_carpools_count
  from {{ ref('operators') }}
),

subsidized_carpools as (
  select COUNT(distinct carpool_v2_id) as subsidized_carpools_count
  from {{ ref('incentives') }}
  where amount > 0
)

select
  fraudulous_carpools.fraudulous_carpools_count,
  valid_carpools.average_carpoolers_by_car,
  valid_carpools.operators_count,
  valid_carpools.validated_carpools_count,
  subsidized_carpools.subsidized_carpools_count

from fraudulous_carpools
cross join valid_carpools
cross join subsidized_carpools
