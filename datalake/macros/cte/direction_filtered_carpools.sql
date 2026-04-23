{% macro direction_filtered_carpools(
  column='j.start_datetime_tz', 
  model_column='carpool_date', 
  type='timestamp', 
  default_start="'2020-01-01 00:00:00'",  
  lookback_nb=3,
  lookback_unit='day'
) %}
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
    j.start_geo_code AS start_arr,
    j.end_geo_code AS end_arr,  
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
    j.is_intra
  FROM {{ ref('trusted_carpools') }} j
  LEFT JOIN {{ ref('trusted_users') }} d ON d.user_id = j.driver_key
  LEFT JOIN {{ ref('trusted_users') }} p ON p.user_id = j.passenger_key
  WHERE {{ time_filter(column, model_column, type, default_start, lookback_nb, lookback_unit) }}
    AND j.valid_acquisition_status = true
    AND ( -- Exclure Paris, Marseille et Lyon par sécurité
      j.start_geo_code NOT IN ('75056', '13055', '69001') OR 
      j.end_geo_code NOT IN ('75056', '13055', '69001') 
    )
{% endmacro %}