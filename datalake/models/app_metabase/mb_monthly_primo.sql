{{ config(
  materialized='view',
  tags=['app_metabase', 'monthly_primo']
) }}

with primo_drivers_by_month as (
  select
    date_trunc('month', first_date_driver)::date as ym,
    count(*)                                     as primo_drivers
  from {{ ref('users') }}
  where first_date_driver is not null
  group by 1
),

primo_passengers_by_month as (
  select
    date_trunc('month', first_date_passenger)::date as ym,
    count(*)                                        as primo_passengers
  from {{ ref('users') }}
  where first_date_passenger is not null
  group by 1
)

select
  coalesce(d.ym, p.ym)            as mois,
  coalesce(d.primo_drivers, 0)    as primo_drivers,
  coalesce(p.primo_passengers, 0) as primo_passengers
from primo_drivers_by_month as d
full join primo_passengers_by_month as p on d.ym = p.ym
order by 1
