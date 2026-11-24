-- Écarts déclaré opérateur / calculé RPC, trajet par trajet, pour une campagne et un mois.
-- Variables psql : camp (policy_id), op (operator_id, 0 = tous), start, end (dates locales,
-- end exclue), zone_col (epci|aom|com|dep|reg|arr), zone_code ('' = pas de filtre zone).
-- Lecture seule (PGOPTIONS='-c default_transaction_read_only=on') : pas de table temporaire,
-- le résultat sort en CSV (psql -o <fichier>) et s'analyse ensuite avec DuckDB.

\set ON_ERROR_STOP on

COPY (
WITH
camp AS (
  SELECT _id, left(territory_siret, 9) AS siren
  FROM zone_trusted.policies
  WHERE _id = :'camp'::int
),

millesime AS (SELECT max(year) AS y FROM zone_trusted.perimeters),

trips AS (
  SELECT c.*
  FROM zone_trusted.carpools AS c
  WHERE
    -- heure locale du trajet, comme l'APDF (tz de la campagne)
    c.start_datetime_tz >= :'start'::timestamp
    AND c.start_datetime_tz < :'end'::timestamp
    AND (:'op'::int = 0 OR c.operator_id = :'op'::int)
),

declared AS (
  SELECT oi.carpool_id, sum(oi.amount)::int AS declared
  FROM zone_trusted.operator_incentives AS oi
  INNER JOIN trips AS t ON t._id = oi.carpool_id
  CROSS JOIN camp
  -- rattachement au financeur par SIREN, comme DataRepositoryProvider (GEN-643)
  WHERE left(oi.siret, 9) = camp.siren
  GROUP BY 1
),

rpc AS (
  SELECT DISTINCT ON (i.carpool_v2_id)
    i.carpool_v2_id, i.result, i.amount, i.status, i.state
  FROM zone_trusted.incentives AS i
  INNER JOIN trips AS t ON t._id = i.carpool_v2_id
  WHERE i.campaign_id = :'camp'::int
  ORDER BY i.carpool_v2_id, i.datetime DESC
)

SELECT
  t._id AS carpool_id,
  t.operator_id,
  t.operator_name,
  t.operator_journey_id,
  t.start_datetime_tz,
  t.start_datetime_tz::date AS day,
  extract(isodow FROM t.start_datetime_tz)::int AS isodow,
  t.operator_class,
  t.distance,
  t.passenger_seats,
  t.passenger_contribution,
  t.driver_key,
  t.passenger_key,
  t.acquisition_status,
  t.fraud_status,
  t.anomaly_status,
  t.fraud_labels,
  t.anomaly_labels,
  t.start_geo_code,
  t.end_geo_code,
  ps.l_arr AS start_l_arr, ps.epci AS start_epci, ps.l_epci AS start_l_epci,
  ps.aom AS start_aom, ps.l_aom AS start_l_aom, ps.reg AS start_reg,
  pe.l_arr AS end_l_arr, pe.epci AS end_epci, pe.l_epci AS end_l_epci,
  pe.aom AS end_aom, pe.l_aom AS end_l_aom, pe.reg AS end_reg,
  coalesce(ps.:"zone_col" = :'zone_code', FALSE) AS zone_start,
  coalesce(pe.:"zone_col" = :'zone_code', FALSE) AS zone_end,
  dc.declared,
  r.result AS rpc_result,
  r.amount AS rpc_amount,
  r.status AS rpc_status,
  -- 1re règle qui s'applique, dans cet ordre
  CASE
    WHEN r.carpool_v2_id IS NULL THEN 'absent_rpc'
    WHEN NOT (
      t.acquisition_status IN ('processed', 'canceled', 'updated')
      AND t.fraud_status IN ('passed', 'failed')
    ) THEN 'hors_apdf_statut'
    WHEN r.status <> 'validated' THEN 'non_valide'
    WHEN t.fraud_status <> 'passed' OR t.anomaly_status <> 'passed' THEN 'fraude_anomalie'
    WHEN coalesce(r.result, 0) = 0 AND coalesce(dc.declared, 0) > 0 THEN 'inegible_rpc'
    WHEN r.result > coalesce(r.amount, 0) THEN 'plafonne'
    WHEN coalesce(dc.declared, 0) = 0 AND r.amount > 0 THEN 'non_declare'
    WHEN dc.declared <> r.amount THEN 'montant_different'
    ELSE 'ok'
  END AS categorie
FROM trips AS t
CROSS JOIN millesime AS m
LEFT JOIN declared AS dc ON dc.carpool_id = t._id
LEFT JOIN rpc AS r ON r.carpool_v2_id = t._id
LEFT JOIN zone_trusted.perimeters AS ps ON ps.arr = t.start_geo_code AND ps.year = m.y
LEFT JOIN zone_trusted.perimeters AS pe ON pe.arr = t.end_geo_code AND pe.year = m.y
WHERE
  (dc.declared > 0 OR r.carpool_v2_id IS NOT NULL)
  AND (
    :'zone_code' = ''
    OR coalesce(ps.:"zone_col" = :'zone_code', FALSE)
    OR coalesce(pe.:"zone_col" = :'zone_code', FALSE)
  )
) TO STDOUT WITH (FORMAT csv, HEADER);

