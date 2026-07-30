{{ config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key='carpool_id',
    tags=['aggregated', 'terms_violation', 'daily']
) }}

-- GEN-647 — refus too_many_trips_by_day : à tort (aucun > 4) vs légitime.
-- Incrémental 7 j ; --full-refresh pour les statuts historiques.

WITH refused AS (
  SELECT
    _id                                                           AS carpool_id,
    operator_id,
    operator_name,
    start_datetime_tz::date                                       AS trip_day,
    created_at                                                    AS refused_at,
    driver_key,
    passenger_key,
    acquisition_status,
    terms_violation_error_labels = ARRAY['too_many_trips_by_day'] AS sole_reason
  FROM {{ ref('carpools') }}
  WHERE
    'too_many_trips_by_day' = ANY(terms_violation_error_labels)
  {% if is_incremental() %}
      AND start_datetime_tz >= (CURRENT_DATE - interval '7 days')
    {% endif %}
),

scope AS (
  SELECT DISTINCT
    operator_id,
    trip_day
  FROM refused
),

-- Trajets d'un usager (conducteur ou passager) par opérateur / jour.
participations AS (
  SELECT
    c.operator_id,
    c.start_datetime_tz::date AS trip_day,
    c.driver_key              AS user_key,
    c.operator_trip_id
  FROM {{ ref('carpools') }} AS c
  INNER JOIN scope AS s
    ON
      c.operator_id = s.operator_id
      AND c.start_datetime_tz::date = s.trip_day
  WHERE c.acquisition_status IS DISTINCT FROM 'canceled'

  UNION

  SELECT
    c.operator_id,
    c.start_datetime_tz::date AS trip_day,
    c.passenger_key           AS user_key,
    c.operator_trip_id
  FROM {{ ref('carpools') }} AS c
  INNER JOIN scope AS s
    ON
      c.operator_id = s.operator_id
      AND c.start_datetime_tz::date = s.trip_day
  WHERE c.acquisition_status IS DISTINCT FROM 'canceled'
),

user_trip_counts AS (
  SELECT
    operator_id,
    trip_day,
    user_key,
    COUNT(DISTINCT operator_trip_id) AS trips
  FROM participations
  GROUP BY operator_id, trip_day, user_key
)

SELECT
  r.carpool_id,
  r.operator_id,
  r.operator_name,
  r.trip_day,
  r.refused_at,
  r.acquisition_status,
  r.sole_reason,
  COALESCE(cd.trips, 0)                                       AS driver_trips,
  COALESCE(cp.trips, 0)
    AS passenger_trips,
  GREATEST(COALESCE(cd.trips, 0), COALESCE(cp.trips, 0))      AS max_trips,
  -- limite « 4 max par usager »
  GREATEST(COALESCE(cd.trips, 0), COALESCE(cp.trips, 0)) <= 4 AS wrongly_flagged
FROM refused AS r
LEFT JOIN user_trip_counts AS cd
  ON
    r.operator_id = cd.operator_id
    AND r.trip_day = cd.trip_day
    AND r.driver_key = cd.user_key
LEFT JOIN user_trip_counts AS cp
  ON
    r.operator_id = cp.operator_id
    AND r.trip_day = cp.trip_day
    AND r.passenger_key = cp.user_key
