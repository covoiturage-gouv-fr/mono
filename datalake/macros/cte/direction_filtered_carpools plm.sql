{% macro direction_filtered_carpools_plm(
  column='j.start_datetime_tz', 
  model_column='carpool_date', 
  type='timestamp', 
  default_start="'2020-01-01 00:00:00'",  
  lookback_nb=3,
  lookback_unit='day'
) %}

WITH plm_perim as (
  SELECT DISTINCT arr, com
  from {{ref('perimeters')}}
  where com is not null and arr <> com
  order by arr asc
)

SELECT
    j._id,
    j.operator_trip_id,
    j.driver_key,
    j.passenger_key,
    j.start_datetime_tz AS carpool_datetime,
    j.hour,
    j.passenger_seats,
    j.distance,
    j.dist_class,   
    COALESCE(plm_s.com, j.start_geo_code) AS start_com,
    COALESCE(plm_e.com, j.end_geo_code) AS end_com,
    -- Nouveaux utilisateurs
    (d.first_date_driver = j.start_datetime_tz::date) AS is_new_driver,
    (p.first_date_passenger = j.start_datetime_tz::date) AS is_new_passenger,
    -- incitations
    j.oi_amount_collectivite,
    j.oi_amount_operator,
    j.oi_amount_other,
    j.oi_amount_total,
    j.oi_collectivite,
    j.oi_operator,
    j.oi_other,
    j.with_incentive,
    (COALESCE(plm_s.com, j.start_geo_code) = COALESCE(plm_e.com, j.end_geo_code)) AS is_intra
  FROM {{ ref('trusted_carpools') }} j
  LEFT JOIN {{ ref('trusted_users') }} d ON d.user_id = j.driver_key
  LEFT JOIN {{ ref('trusted_users') }} p ON p.user_id = j.passenger_key
  LEFT JOIN plm_perim plm_s ON plm_s.arr = j.start_geo_code
  LEFT JOIN plm_perim plm_e ON plm_e.arr = j.end_geo_code
  WHERE {{ time_filter(column, model_column, type, default_start, lookback_nb, lookback_unit) }}
    AND j.valid_acquisition_status = true
    AND ( 
      plm_s.com IS NOT NULL OR 
      plm_e.com IS NOT NULL
    )

{% endmacro %}