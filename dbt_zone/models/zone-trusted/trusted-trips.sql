{{ 
    config(
    materialized = 'table' if target.name == 'dev' else 'incremental',
    incremental_strategy = 'delete+insert',
    unique_key=[ 'operator_journey_id', '_id'],
    indexes = [
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
WITH temp AS (
  SELECT
    cc.*,
    oo.name
      AS operator_name,
    oo.siret
      AS operator_siret,
    cs._id
      AS status_id,
    cs.updated_at
      AS status_updated_at,
    cs.acquisition_status::varchar
      AS acquisition_status,
    cs.fraud_status::varchar
      AS fraud_status,
    cs.anomaly_status::varchar
      AS anomaly_status,
    (
      cs.acquisition_status IN (
        'processed', 'failed', 'canceled', 'expired', 'terms_violation_error'
      )
    )::boolean                     AS final_status,
    (
      cs.acquisition_status = 'processed'
      AND cs.anomaly_status = 'passed'
      AND cs.fraud_status = 'passed'
    )::boolean
      AS valid_status,
    cg._id
      AS geo_id,
    cg.start_geo_code,
    cg.end_geo_code,
    cg.updated_at
      AS geo_updated_at,
    cg.errors
      AS geo_errors,
    coalesce(
      cc.driver_identity_key,
      cc.driver_operator_user_id,
      cc.driver_phone,
      cc.driver_phone_trunc
    )                              AS driver_id,
    coalesce(
      cc.passenger_identity_key,
      cc.passenger_operator_user_id,
      cc.passenger_phone,
      cc.passenger_phone_trunc
    )                              AS passenger_id,
    CASE
      WHEN
        cg.start_geo_code::text ~ '^97[1-2]'
        THEN cc.start_datetime AT TIME ZONE 'America/Guadeloupe'
      WHEN
        cg.start_geo_code::text ~ '^973'
        THEN cc.start_datetime AT TIME ZONE 'America/Cayenne'
      WHEN
        cg.start_geo_code::text ~ '^974'
        THEN cc.start_datetime AT TIME ZONE 'Indian/Reunion'
      WHEN
        cg.start_geo_code::text ~ '^976'
        THEN cc.start_datetime AT TIME ZONE 'Indian/Mayotte'
      ELSE cc.start_datetime AT TIME ZONE 'Europe/Paris'
    END
      AS tz_start_datetime,
    CASE
      WHEN
        cg.end_geo_code::text ~ '^97[1-2]'
        THEN cc.end_datetime AT TIME ZONE 'America/Guadeloupe'
      WHEN
        cg.end_geo_code::text ~ '^973'
        THEN cc.end_datetime AT TIME ZONE 'America/Cayenne'
      WHEN
        cg.end_geo_code::text ~ '^974'
        THEN cc.end_datetime AT TIME ZONE 'Indian/Reunion'
      WHEN
        cg.end_geo_code::text ~ '^976'
        THEN cc.end_datetime AT TIME ZONE 'Indian/Mayotte'
      ELSE cc.end_datetime AT TIME ZONE 'Europe/Paris'
    END
      AS tz_end_datetime
  FROM {{ source('carpool', 'carpools') }} AS cc
  LEFT JOIN {{ source('carpool', 'status') }} AS cs ON cc._id = cs.carpool_id
  LEFT JOIN {{ source('carpool', 'geo') }} AS cg ON cc._id = cg.carpool_id
  LEFT JOIN
    {{ source('operator', 'operators') }} AS oo
    ON cc.operator_id = oo._id
  WHERE
    date_part('year', cc.start_datetime) >= 2020
    AND cc.start_datetime < now() - INTERVAL '2' DAY
  {% if is_incremental() %}
    AND cc.start_datetime::date >= (
          SELECT COALESCE(MAX(start_datetime), DATE '1970-01-01')
          FROM {{ this }}
    )::date
  {% endif %}

  {% if target.name == 'dev' %}
    LIMIT 5000
  {% endif %}
),

temp_agg AS (
  SELECT
    t._id,
    t.operator_journey_id,
    array_agg(fl.label) AS fraud_labels,
    array_agg(al.label) AS anomaly_labels,
    sum(pi.amount)      AS sum_incentives
  FROM temp AS t
  LEFT JOIN {{ source('fraudcheck', 'labels') }} AS fl ON t._id = fl.carpool_id
  LEFT JOIN {{ source('anomaly', 'labels') }} AS al ON t._id = al.carpool_id
  LEFT JOIN
    {{ source('policy', 'incentives') }} AS pi
    ON t.operator_journey_id = pi.operator_journey_id
  GROUP BY
    1, 2
)

SELECT
  a.*,
  b.*
FROM temp AS a
LEFT JOIN temp_agg AS b ON a._id = b._id
