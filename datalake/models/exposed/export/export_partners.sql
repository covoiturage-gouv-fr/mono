{{ config(
    materialized='view',
    tags=['exposed', 'export', 'partenaires']
) }}

WITH latest_perimeters AS (
  SELECT DISTINCT ON (arr)
    arr,
    com,
    dep,
    l_dep,
    epci,
    l_epci,
    aom,
    l_aom,
    reg,
    l_reg,
    country,
    CASE WHEN arr <> country THEN l_arr END                AS l_arr,
    CASE WHEN arr <> country THEN l_country ELSE l_arr END AS l_country,
    CASE
      WHEN surface > 0 AND (pop::float / (surface / 100.0)) > 40 THEN 3
      ELSE 2
    END                                                    AS precision
  FROM {{ ref('perimeters') }}
  ORDER BY arr ASC, year DESC
)

SELECT
  c.legacy_id
    AS journey_id,
  c.operator_trip_id,
  c.operator_journey_id,
  c.operator_class,
  c.acquisition_status,
  c.fraud_status,
  c.anomaly_status,
  c.operator_id,

  -- date filtering column (raw DATE in Europe/Paris)
  c.start_datetime_tz::date
    AS start_date_filter,

  -- raw timestamps for sargable filtering (not emitted in CSV)
  c.start_datetime_tz,
  c.start_datetime
    AS start_datetime_utc,

  c.start_geo_code
    AS start_insee,
  gps.arr
    AS start_arr,
  gps.com
    AS start_com,
  gps.dep
    AS start_dep,
  gps.epci
    AS start_epci_code,
  gps.aom
    AS start_aom_code,
  gps.reg
    AS start_reg,
  gps.country
    AS start_country,

  c.end_geo_code
    AS end_insee,

  gpe.arr
    AS end_arr,
  gpe.com
    AS end_com,
  gpe.dep
    AS end_dep,
  gpe.epci
    AS end_epci_code,

  gpe.aom
    AS end_aom_code,
  gpe.reg
    AS end_reg,
  gpe.country
    AS end_country,
  gps.l_arr
    AS start_commune,
  gps.l_dep
    AS start_departement,
  gps.l_epci
    AS start_epci,
  gps.l_aom
    AS start_aom,
  gps.l_reg
    AS start_region,

  gps.l_country
    AS start_pays,
  gpe.l_arr
    AS end_commune,
  gpe.l_dep
    AS end_departement,
  gpe.l_epci
    AS end_epci,
  gpe.l_aom
    AS end_aom,
  gpe.l_reg
    AS end_region,
  gpe.l_country
    AS end_pays,
  c.operator_name
    AS operator,

  sc.passenger_operator_user_id
    AS operator_passenger_id,
  sc.passenger_identity_key,
  sc.driver_operator_user_id
    AS operator_driver_id,
  sc.driver_identity_key,
  c.passenger_seats,
  TRUE
    AS offer_public,

  c.start_datetime
    AS offer_accepted_at,
  'normal'::text
    AS incentive_type,

  -- combined carpool status (port of castToStatusEnum precedence)
  CASE
    WHEN c.acquisition_status IS NULL THEN 'unknown'
    WHEN c.acquisition_status = 'canceled' THEN 'canceled'
    WHEN c.acquisition_status IN ('expired', 'terms_violation_error')
      THEN 'terms_violation_error'
    WHEN c.acquisition_status = 'failed' THEN 'acquisition_error'
    WHEN
      c.acquisition_status = 'processed'
      AND c.anomaly_status = 'passed'
      AND c.fraud_status = 'passed' THEN 'ok'
    WHEN c.fraud_status = 'failed' THEN 'fraud_error'
    WHEN c.anomaly_status = 'failed' THEN 'anomaly_error'
    ELSE 'pending'
  END
    AS status,
  to_char(ts_ceil(c.start_datetime_tz, 600), 'YYYY-MM-DD HH24:MI:SS')
    AS start_datetime,
  to_char(ts_ceil(c.start_datetime_tz, 600), 'YYYY-MM-DD')
    AS start_date,
  to_char(ts_ceil(c.start_datetime_tz, 600), 'HH24:MI:SS')
    AS start_time,

  to_char(ts_ceil(c.end_datetime_tz, 600), 'YYYY-MM-DD HH24:MI:SS')
    AS end_datetime,
  to_char(ts_ceil(c.end_datetime_tz, 600), 'YYYY-MM-DD')
    AS end_date,
  to_char(ts_ceil(c.end_datetime_tz, 600), 'HH24:MI:SS')
    AS end_time,
  to_char((c.duration || ' seconds')::interval, 'HH24:MI:SS')
    AS duration,
  c.distance::float
  / 1000
    AS distance,

  -- ::float8 : rendu csv de l'API (numeric tronque -> float, sans zeros)
  trunc(st_y(sc.start_position::geometry)::numeric, gps.precision)::float8
    AS start_lat,
  trunc(st_x(sc.start_position::geometry)::numeric, gps.precision)::float8
    AS start_lon,
  trunc(st_y(sc.end_position::geometry)::numeric, gpe.precision)::float8
    AS end_lat,

  trunc(st_x(sc.end_position::geometry)::numeric, gpe.precision)::float8
    AS end_lon,

  -- operator incentives (spread from JSONB array)
  c.driver_revenue::float
  / 100
    AS driver_revenue,
  c.passenger_contribution::float
  / 100
    AS passenger_contribution,
  -- '1'/'' reproduit le rendu csv-stringify de l'API (cast booleen -> "1"/"")
  CASE WHEN cee._id IS NOT NULL THEN '1' ELSE '' END
    AS cee_application,
  -- 3 premières incitations opérateur (oi_details) puis RPC (campaigns)
  {% for i in range(3) %}
    {% for f in ['siret', 'name', 'amount'] %}
      c.oi_details[{{ i }}]
      ->> '{{ f }}'
        AS incentive_{{ i }}_{{ f }},
    {% endfor %}
  {% endfor %}
  {% for i in range(3) %}
    {% set last_i = loop.last %}
    {% for f in ['campaign_id', 'campaign_name', 'siret', 'name', 'amount'] %}
      c.campaigns[{{ i }}]
      ->> '{{ f }}'
        AS incentive_rpc_{{ i }}_{{ f }}
      {%- if not (last_i and loop.last) %},{% endif %}
    {% endfor %}
  {% endfor %}

FROM {{ ref('carpools') }} AS c
LEFT JOIN
  {{ source('dlk_import', 'carpool_v2_carpools') }} AS sc
  ON c._id = sc._id
LEFT JOIN latest_perimeters AS gps ON c.start_geo_code = gps.arr
LEFT JOIN latest_perimeters AS gpe ON c.end_geo_code = gpe.arr
LEFT JOIN
  {{ ref('cee') }} AS cee
  ON c._id = cee.carpool_v2_id
WHERE c.valid_acquisition_status = TRUE
