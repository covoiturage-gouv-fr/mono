{% macro filtered_carpools_h3(
  column='j.start_datetime_tz', 
  model_column='incremental_date', 
  type='timestamp', 
  default_start="'2020-01-01 00:00:00'",  
  lookback_nb=0,
  lookback_unit='day'
) %}
SELECT
    j._id,
    j.operator_id,
    j.operator_trip_id,
    j.operator_class,
    j.driver_key,
    j.passenger_key,
    j.start_datetime_tz AS carpool_datetime,
    j.hour,
    j.driver_revenue,
    j.passenger_seats,
    j.passenger_over_18,
    j.passenger_contribution,
    j.distance,
    j.dist_class,    
    j.start_h3_index AS start_h3,
    j.end_h3_index AS end_h3,
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
    j.campaigns,
    j.campaigns_amount_total,
    j.campaigns_result_total,
    j.is_intra
  FROM {{ ref('trusted_carpools') }} j
  LEFT JOIN {{ ref('trusted_users') }} d ON d.user_id = j.driver_key
  LEFT JOIN {{ ref('trusted_users') }} p ON p.user_id = j.passenger_key
  WHERE {{ time_filter(column, model_column, type, default_start, lookback_nb, lookback_unit) }}
    AND j.valid_acquisition_status = true
    AND ( -- Exclure Paris, Marseille et Lyon par sécurité
      j.start_h3_index IS NOT NULL OR 
      j.end_h3_index IS NOT NULL
    )
{% endmacro %}