{% macro direction_filtered_carpools_aomreg(
  column='j.start_datetime_tz', 
  model_column='carpool_date', 
  type='timestamp', 
  default_start="'2020-01-01 00:00:00'",  
  lookback_nb=0,
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
    aomr_s.aom AS start_aomreg,
    aomr_e.aom AS end_aomreg,
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
    (ps.aom = pe.aom) AS is_intra
  FROM {{ ref('trusted_carpools') }} j
  LEFT JOIN {{ ref('trusted_users') }} d ON d.user_id = j.driver_key
  LEFT JOIN {{ ref('trusted_users') }} p ON p.user_id = j.passenger_key
  LEFT JOIN {{ref('perimeters')}} ps ON ps.arr = j.start_geo_code AND ps.year = EXTRACT('year' FROM j.start_datetime_tz)
  LEFT JOIN {{ref('perimeters')}} pe ON pe.arr = j.end_geo_code AND pe.year = EXTRACT('year' FROM j.end_datetime_tz)
  LEFT JOIN {{ ref('aom_region') }} aomr_s ON aomr_s.reg = ps.reg AND ps.year = EXTRACT('year' FROM j.start_datetime_tz)
  LEFT JOIN {{ ref('aom_region') }} aomr_e ON aomr_e.reg = pe.reg AND pe.year = EXTRACT('year' FROM j.end_datetime_tz)
  WHERE {{ time_filter(column, model_column, type, default_start, lookback_nb, lookback_unit) }}
    AND j.valid_acquisition_status = true
    AND ( 
      aomr_s.aom IS NOT NULL OR
      aomr_e.aom IS NOT NULL
    )
    AND ps.aom <> pe.aom -- Exclude intra AOM classiques

{% endmacro %}