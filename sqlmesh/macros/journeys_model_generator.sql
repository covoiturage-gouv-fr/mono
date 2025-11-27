{% macro journeys_model_generator(start_date, end_date) %}
    WITH carpools AS (
      SELECT *
      FROM carpool_v2.carpools
      WHERE start_datetime BETWEEN ({{start_date}}::timestamp - INTERVAL '1 day') AND ({{end_date}}::timestamp + INTERVAL '1 day')
    ),

    perimeters_retablissement AS (
      SELECT com, geom 
      FROM trusted_zone.perimeters
      WHERE year = EXTRACT(YEAR FROM {{start_date}}::date)
        AND com IN (
          SELECT DISTINCT new_com 
          FROM trusted_zone.com_evolution 
          WHERE year = EXTRACT(YEAR FROM {{start_date}}::date) 
            AND mod = 21
        )
    ),

    geocoding AS (
      -- Cas 1: Communes non fusionnées (mod = 32)
      SELECT
        g.carpool_id,
        COALESCE(cs.new_com, g.start_geo_code) AS start_geo_code,
        COALESCE(ce.new_com, g.end_geo_code) AS end_geo_code,
        g.updated_at AS geo_updated_at,
        g.errors AS geo_errors
      FROM carpool_v2.geo AS g
      INNER JOIN carpools AS c ON g.carpool_id = c._id
      LEFT JOIN trusted_zone.com_evolution cs 
        ON g.start_geo_code = cs.old_com 
        AND cs.mod = 32 
        AND cs.year = EXTRACT(YEAR FROM {{start_date}}::date)
      LEFT JOIN trusted_zone.com_evolution ce 
        ON g.end_geo_code = ce.old_com 
        AND ce.mod = 32 
        AND ce.year = EXTRACT(YEAR FROM {{start_date}}::date)
      WHERE g.start_geo_code NOT IN (
        SELECT DISTINCT old_com 
        FROM trusted_zone.com_evolution 
        WHERE year = EXTRACT(YEAR FROM {{start_date}}::date) AND mod = 21
      )
        OR g.end_geo_code NOT IN (
        SELECT DISTINCT old_com 
        FROM trusted_zone.com_evolution 
        WHERE year = EXTRACT(YEAR FROM {{start_date}}::date) AND mod = 21
      )

      UNION ALL

      -- Cas 2: Communes fusionnées (mod = 21) - utiliser perimetres
      SELECT
        g.carpool_id,
        COALESCE(ps.com, g.start_geo_code) AS start_geo_code,
        COALESCE(pe.com, g.end_geo_code) AS end_geo_code,
        g.updated_at AS geo_updated_at,
        g.errors AS geo_errors
      FROM carpool_v2.geo g
      INNER JOIN carpools AS c ON g.carpool_id = c._id
      LEFT JOIN perimeters_retablissement ps 
        ON ST_Intersects(c.start_position::geometry, ps.geom)
      LEFT JOIN perimeters_retablissement pe 
        ON ST_Intersects(c.end_position::geometry, pe.geom)
      WHERE g.start_geo_code IN (
        SELECT DISTINCT old_com 
        FROM trusted_zone.com_evolution 
        WHERE year = EXTRACT(YEAR FROM {{start_date}}::date) AND mod = 21
      )
        OR g.end_geo_code IN (
        SELECT DISTINCT old_com 
        FROM trusted_zone.com_evolution 
        WHERE year = EXTRACT(YEAR FROM {{start_date}}::date) AND mod = 21
      )
    ),

    carpools_status AS (
      SELECT
        s.carpool_id,
        s.updated_at AS status_updated_at,
        s.acquisition_status,
        s.fraud_status,
        s.anomaly_status,
        COALESCE(
          s.acquisition_status IN {{get_final_acquisition_status_list()}},
          FALSE
        ) AS final_acquisition_status,
        COALESCE(
          s.acquisition_status = 'processed'
          AND s.anomaly_status = 'passed'
          AND s.fraud_status = 'passed',
          FALSE
        ) AS valid_acquisition_status
      FROM carpool_v2.status AS s
      INNER JOIN carpools AS c ON s.carpool_id = c._id
    ),

    fraud_labels AS (
      SELECT
        fl.carpool_id,
        ARRAY_AGG(fl.label) AS fraud_labels
      FROM fraudcheck.labels AS fl
      INNER JOIN carpools AS c ON fl.carpool_id = c._id
      GROUP BY 1
    ),

    anomaly_labels AS (
      SELECT
        al.carpool_id,
        ARRAY_AGG(al.label) AS anomaly_labels
      FROM fraudcheck.labels AS al
      INNER JOIN carpools AS c ON al.carpool_id = c._id
      GROUP BY 1
    ),

    operator_incentives AS (
      SELECT
        oi.carpool_id,
        ARRAY_AGG(DISTINCT oi.siret) AS operator_incentives_sirets,
        SUM(oi.amount) AS operator_incentives_amount_total
      FROM carpool_v2.operator_incentives AS oi
      INNER JOIN carpools AS c ON oi.carpool_id = c._id
      WHERE oi.amount > 0
      GROUP BY 1
    ),

    policy_incentives AS (
      SELECT
        pi.carpool_id,
        pi.policy_id,
        SUM(pi.amount) AS policy_incentives_amount_total,
        SUM(pi.result) AS policy_incentives_result_total
      FROM policy.incentives AS pi
      INNER JOIN carpools AS c ON pi.carpool_id = c._id
      WHERE pi.status = 'validated'
      GROUP BY 1, 2
    )

    SELECT
      c._id,
      c.created_at,
      c.updated_at,
      c.operator_id,
      o.name AS operator_name,
      o.siret AS operator_siret,
      c.operator_journey_id,
      c.operator_trip_id,
      c.operator_class,
      c.start_datetime,
      {{get_timezoned_timestamp("g.start_geo_code", "c.start_datetime")}} AS start_datetime_tz,
      c.start_position,
      h3_lat_lng_to_cell((c.start_position::geometry)::point, 9) AS start_h3_index,
      g.start_geo_code,
      c.end_datetime,
      {{get_timezoned_timestamp("g.end_geo_code", "c.end_datetime")}} AS end_datetime_tz,
      c.end_position,
      h3_lat_lng_to_cell((c.end_position::geometry)::point, 9) AS end_h3_index,
      g.end_geo_code,
      g.geo_errors,
      g.geo_updated_at,
      c.distance,
      c.licence_plate,
      c.driver_identity_key,
      c.driver_operator_user_id,
      c.driver_phone,
      c.driver_phone_trunc,
      c.driver_travelpass_name,
      c.driver_travelpass_user_id,
      c.driver_revenue,
      c.passenger_identity_key,
      c.passenger_operator_user_id,
      c.passenger_phone,
      c.passenger_phone_trunc,
      c.passenger_travelpass_name,
      c.passenger_travelpass_user_id,
      c.passenger_over_18,
      c.passenger_seats,
      c.passenger_contribution,
      c.passenger_payments,
      oi.operator_incentives_sirets,
      oi.operator_incentives_amount_total,
      pi.policy_id,
      pi.policy_incentives_amount_total,
      pi.policy_incentives_result_total,
      cs.fraud_status,
      fl.fraud_labels,
      cs.anomaly_status,
      al.anomaly_labels,
      cs.acquisition_status,
      cs.status_updated_at,
      cs.final_acquisition_status,
      cs.valid_acquisition_status,
      c.uuid,
      c.legacy_id
    FROM carpools AS c
    LEFT JOIN carpools_status AS cs ON c._id = cs.carpool_id
    LEFT JOIN geocoding AS g ON c._id = g.carpool_id
    LEFT JOIN fraud_labels AS fl ON c._id = fl.carpool_id
    LEFT JOIN anomaly_labels AS al ON c._id = al.carpool_id
    LEFT JOIN operator_incentives AS oi ON c._id = oi.carpool_id
    LEFT JOIN policy_incentives AS pi ON c._id = pi.carpool_id
    LEFT JOIN operator.operators AS o ON c.operator_id = o._id
    WHERE {{get_timezoned_timestamp("g.start_geo_code", "c.start_datetime")}} BETWEEN {{start_date}}::timestamp AND {{end_date}}::timestamp;
{% endmacro %}