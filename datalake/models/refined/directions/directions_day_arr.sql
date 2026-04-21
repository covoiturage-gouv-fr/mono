{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key=['code', 'type', 'journey_date', 'direction'],
    indexes = [
      { 'columns':['code', 'type', 'journey_date', 'direction'], 'unique': true },
    ],
    tags=['refined', 'directions', 'day_arr']
) }}

  SELECT
    j._id,
    j.driver_key,
    j.passenger_key,
    j.start_datetime_tz::date AS journey_date,
    EXTRACT('hour' FROM j.start_datetime_tz)::int AS hour,
    j.passenger_seats,
    j.distance,
    CASE
      WHEN j.distance < 10000 THEN '0-10'
      WHEN j.distance < 20000 THEN '10-20'
      WHEN j.distance < 30000 THEN '20-30'
      WHEN j.distance < 40000 THEN '30-40'
      WHEN j.distance < 50000 THEN '40-50'
      ELSE '>50'
    END AS dist_class,   
    j.start_geo_code AS start_arr,
    j.end_geo_code AS end_arr,  
    -- Nouveaux utilisateurs
    (d.first_date_driver = j.start_datetime_tz::date) AS is_new_driver,
    (p.first_date_passenger = j.start_datetime_tz::date) AS is_new_passenger
    -- incitations
    COALESCE(j.oi_amount_collectivite, 0) AS oi_amount_collectivite,
    COALESCE(j.oi_amount_operator, 0) AS oi_amount_operator,
    COALESCE(j.oi_amount_other, 0) AS oi_amount_other,
    COALESCE(j.oi_amount_total, 0) AS oi_amount_total,
    COALESCE(j.oi_collectivite, 0) AS oi_collectivite,
    COALESCE(j.oi_operator, 0) AS oi_operator,
    COALESCE(j.oi_other, 0) AS oi_other,
    (COALESCE(j.oi_amount_total, 0) > 0) as with_incentive
  FROM {{ ref('trusted_journeys') }} j
  LEFT JOIN {{ ref('trusted_users') }} d ON d.user_id = j.driver_key
  LEFT JOIN {{ ref('trusted_users') }} p ON p.user_id = j.passenger_key
  WHERE {{ time_filter('j.start_datetime_tz', 'start_datetime_tz', type='date', default_start="'2020-01-01'") }}
    AND j.valid_acquisition_status = true
