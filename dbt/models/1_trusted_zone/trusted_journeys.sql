{{ 
    config(
    materialized = 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key=['_id'],
    indexes = [
      {
        'columns':["_id"],
        'unique':true
      },
      {
        'columns':[
          'updated_at'
        ],
      },
      {
        'columns':[
          'geo_updated_at'
        ],
      },
      {
        'columns':[
          'status_updated_at'
        ],
      },
      {
        'columns':[
          'start_datetime'
        ],
      },
      {
        'columns':[
          'operator_journey_id'
        ],
      }
    ]
  )
}}


with carpools as ( -- join carpools with all datasets
  select
    c.*,
    -- Add data from other sources
    s.updated_at
    as status_updated_at,
    s.acquisition_status,
    s.fraud_status,
    s.anomaly_status,
    g.start_geo_code,
    g.end_geo_code,
    g.updated_at
    as geo_updated_at,
    g.errors
    as geo_errors,
    -- datetimes are timezoned
    {{ get_timezoned_timestamp('g.start_geo_code','c.start_datetime') }} as start_datetime_tz,
    {{ get_timezoned_timestamp('g.end_geo_code','c.end_datetime') }} as end_datetime_tz,
    -- Enrich with useful status columns
    coalesce(
      s.acquisition_status in {{ get_final_acquisition_status_list() }},
      FALSE
    )            as journey_has_final_acquisition_status,
    coalesce(
      s.acquisition_status = 'processed'
      and s.anomaly_status = 'passed'
      and s.fraud_status = 'passed',
      FALSE
    )
    as journey_has_valid_acquisition_status
  from {{ source('carpool', 'carpools') }} as c
  left join {{ source('carpool', 'status') }} as s on c._id = s.carpool_id
  left join {{ source('carpool', 'geo') }} as g on c._id = g.carpool_id
  {% if is_incremental() %} -- in case of incremental, only select relevant carpools
    where c.updated_at >= coalesce(
      (select least(max(updated_at),max(geo_updated_at),max(status_updated_at)) from {{this}}), '1970-01-01'
    ) -- Updates
    or (c."_id" in (select "_id" from {{this}} where geo_updated_at is null)) -- new rows that does not have geo data yet
    or (c."_id" in (select "_id" from {{this}} where status_updated_at is null)) -- new rows that does not have status data yet
  {% endif %}
),

-- The following datasets have not updated_at column
fraud_labels as ( -- Pre-aggregate fraud labels for selected carpools
  select
    fl.carpool_id,
    array_agg(fl.label) as fraud_labels
  from {{ source('fraudcheck', 'labels') }} as fl
  {% if is_incremental() %}
    inner join carpools as c on fl.carpool_id = c._id
  {% endif %}
  group by 1
),

anomaly_labels as ( -- Pre-aggregate anomaly labels for selected carpools
  select
    al.carpool_id,
    array_agg(al.label) as anomaly_labels
  from {{ source('fraudcheck', 'labels') }} as al
  {% if is_incremental() %}
    inner join carpools as c on al.carpool_id = c._id
  {% endif %}
  group by 1
),

-- Pre-aggregate operator incentives for selected carpools
operator_incentives as (
  select
    oi.carpool_id,
    array_agg(distinct oi.siret) as operator_incentives_sirets,
    sum(oi.amount)               as operator_incentives_amount_total
  from {{ source('carpool', 'operator_incentives') }} as oi
  {% if is_incremental() %}
    inner join carpools as c on oi.carpool_id = c._id
  {% endif %}
  where oi.amount > 0 -- Filter out zero-amounts as should be nulls
  group by 1
),

joined_data as ( -- Join carpool enriched data with aggregated datasets
  select
    c.*,
    o.name
    as operator_name,
    o.siret
    as operator_siret,
    fl.fraud_labels,
    al.anomaly_labels,
    oi.operator_incentives_sirets,
    oi.operator_incentives_amount_total
  from carpools as c
  left join fraud_labels as fl on c._id = fl.carpool_id
  left join anomaly_labels as al on c._id = al.carpool_id
  left join operator_incentives as oi on c._id = oi.carpool_id
  left join
    {{ source('operator', 'operators') }} as o
    on c.operator_id = o._id
)

select
  _id,
  created_at,
  updated_at,
  operator_id,
  operator_name,
  operator_siret,
  operator_journey_id,
  operator_trip_id,
  operator_class,
  start_datetime,
  start_datetime_tz,
  start_position,
  start_geo_code,
  end_datetime,
  end_datetime_tz,
  end_position,
  end_geo_code,
  geo_errors,
  geo_updated_at,
  distance,
  licence_plate,
  driver_identity_key,
  driver_operator_user_id,
  driver_phone,
  driver_phone_trunc,
  driver_travelpass_name,
  driver_travelpass_user_id,
  driver_revenue,
  passenger_identity_key,
  passenger_operator_user_id,
  passenger_phone,
  passenger_phone_trunc,
  passenger_travelpass_name,
  passenger_travelpass_user_id,
  passenger_over_18,
  passenger_seats,
  passenger_contribution,
  passenger_payments,
  operator_incentives_sirets,
  operator_incentives_amount_total,
  status_updated_at,
  acquisition_status,
  fraud_status,
  fraud_labels,
  anomaly_status,
  anomaly_labels,
  journey_has_final_acquisition_status,
  journey_has_valid_acquisition_status,
  uuid,
  legacy_id
from joined_data
