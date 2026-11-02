{#
  Stats financières par AOM (direction both) des vues app_metabase mb_aom_* : incitations
  versées par l'AOM du périmètre (self) ou par une autre AOM (other), ventilées intra/inter.

  Le self/other se lit sur les lignes normalisées operator_incentives / incentives (une par
  trajet x financeur), pas sur les jsonb oi_details / campaigns de carpools.
  Une collectivité EPCI qui n'est pas AOM n'est ni self ni other.
#}
{% macro aom_financials_model(perim, grain) %}

  {% set lookbacks = {
    'day':   {'nb': 3, 'unit': 'day'},
    'month': {'nb': 1, 'unit': 'month'}
  } %}

  {% if perim not in ['aom', 'aomreg'] or grain not in lookbacks %}
    {{ exceptions.raise_compiler_error("aom_financials_model : perim aom|aomreg et grain day|month attendus, reçu " ~ perim ~ "/" ~ grain) }}
  {% endif %}

  {% set lb = lookbacks[grain] %}

{{ config(
  materialized='incremental',
  incremental_strategy='delete+insert',
  unique_key=['code', 'incremental_date'],
  indexes=[
    {'columns': ['code', 'incremental_date'], 'unique': true}
  ],
  tags=['aggregated', 'aom_financials', grain, perim, 'both', 'daily']
) }}

WITH filtered_carpools AS (
  {{ filtered_carpools(perim, lookback_nb=lb.nb, lookback_unit=lb.unit, with_new_users=false, strict=true) }}
),

exploded AS (
  SELECT *, start_code AS code
  FROM filtered_carpools
  WHERE start_code IS NOT NULL
  UNION ALL
  SELECT *, end_code AS code
  FROM filtered_carpools
  WHERE end_code IS NOT NULL
  AND NOT is_intra
),

aoms AS (
  SELECT DISTINCT code
  FROM {{ ref('perimeters_agg') }}
  WHERE type = 'aom'
),

-- une ligne par (trajet, SIREN d'AOM financeur)
aom_incentives AS (
  SELECT
    oi.carpool_id,
    LEFT(oi.siret, 9) AS siren,
    SUM(oi.amount) AS amount
  FROM {{ ref('operator_incentives') }} AS oi
  INNER JOIN aoms ON aoms.code = LEFT(oi.siret, 9)
  WHERE oi.carpool_id IN (SELECT _id FROM filtered_carpools)
  GROUP BY 1, 2
),

aom_incentives_by_carpool AS (
  SELECT
    carpool_id,
    SUM(amount) AS amount,
    COUNT(*) AS sirens
  FROM aom_incentives
  GROUP BY 1
),

-- une ligne par (trajet, SIREN du territoire de la campagne RPC)
campaign_incentives AS (
  SELECT
    carpool_v2_id AS carpool_id,
    LEFT(territory_siret, 9) AS siren,
    SUM(amount) AS amount,
    SUM(result) AS result
  FROM {{ ref('incentives') }}
  WHERE
    carpool_v2_id IN (SELECT _id FROM filtered_carpools)
    AND territory_siret IS NOT NULL
  GROUP BY 1, 2
),

campaign_incentives_by_carpool AS (
  SELECT
    carpool_id,
    SUM(amount) AS amount,
    SUM(result) AS result
  FROM campaign_incentives
  GROUP BY 1
),

split AS (
  SELECT
    e.*,
    COALESCE(ai_self.amount, 0) AS aom_self_amount,
    COALESCE(ai_all.amount, 0) - COALESCE(ai_self.amount, 0) AS aom_other_amount,
    ai_self.carpool_id IS NOT NULL AS aom_self_exists,
    COALESCE(ai_all.sirens, 0) > (ai_self.carpool_id IS NOT NULL)::int AS aom_other_exists,
    COALESCE(ci_self.amount, 0) AS ci_amount_self,
    COALESCE(ci_all.amount, 0) - COALESCE(ci_self.amount, 0) AS ci_amount_other,
    COALESCE(ci_self.result, 0) AS ci_result_self,
    COALESCE(ci_all.result, 0) - COALESCE(ci_self.result, 0) AS ci_result_other
  FROM exploded AS e
  LEFT JOIN aom_incentives AS ai_self
    ON ai_self.carpool_id = e._id AND ai_self.siren = e.code
  LEFT JOIN aom_incentives_by_carpool AS ai_all
    ON ai_all.carpool_id = e._id
  LEFT JOIN campaign_incentives AS ci_self
    ON ci_self.carpool_id = e._id AND ci_self.siren = e.code
  LEFT JOIN campaign_incentives_by_carpool AS ci_all
    ON ci_all.carpool_id = e._id
)

SELECT
  code,
  {{ incremental_columns('carpool_datetime', grain) }},
  COUNT(*) AS carpools,
  COUNT(*) FILTER (WHERE is_intra) AS intra_carpools,
  SUM(driver_revenue) AS driver_revenue,
  SUM(driver_revenue) FILTER (WHERE COALESCE(is_intra, false)) AS driver_revenue_intra,
  SUM(driver_revenue) FILTER (WHERE NOT COALESCE(is_intra, false)) AS driver_revenue_inter,
  SUM(passenger_contribution) AS passenger_contribution,
  SUM(passenger_contribution) FILTER (WHERE COALESCE(is_intra, false)) AS passenger_contribution_intra,
  SUM(passenger_contribution) FILTER (WHERE NOT COALESCE(is_intra, false)) AS passenger_contribution_inter,
  percentile_cont(0.25) WITHIN GROUP (ORDER BY distance) AS q1_distance,
  percentile_cont(0.5) WITHIN GROUP (ORDER BY distance) AS median_distance,
  percentile_cont(0.75) WITHIN GROUP (ORDER BY distance) AS q3_distance,
  COUNT(*) FILTER (WHERE NOT with_incentive) AS no_oi,
  COUNT(*) FILTER (WHERE oi_operator > 0) AS carpools_operator_incentive,
  COUNT(*) FILTER (WHERE oi_operator > 0 AND oi_collectivite = 0 AND oi_other = 0) AS oi_operator_only,
  COUNT(*) FILTER (WHERE oi_other > 0) AS carpools_other_incentive,
  COUNT(*) FILTER (WHERE aom_self_exists) AS carpools_collectivite_self_incentive,
  COUNT(*) FILTER (WHERE aom_other_exists) AS carpools_collectivite_other_incentive,
  SUM(oi_amount_total) AS oi_amount_total,
  SUM(oi_amount_total) FILTER (WHERE COALESCE(is_intra, false)) AS oi_amount_total_intra,
  SUM(oi_amount_total) FILTER (WHERE NOT COALESCE(is_intra, false)) AS oi_amount_total_inter,
  SUM(oi_amount_operator) AS oi_amount_operator,
  SUM(oi_amount_operator) FILTER (WHERE COALESCE(is_intra, false)) AS oi_amount_operator_intra,
  SUM(oi_amount_operator) FILTER (WHERE NOT COALESCE(is_intra, false)) AS oi_amount_operator_inter,
  SUM(aom_self_amount) AS oi_amount_collectivite_self,
  SUM(aom_self_amount) FILTER (WHERE COALESCE(is_intra, false)) AS oi_amount_collectivite_self_intra,
  SUM(aom_self_amount) FILTER (WHERE NOT COALESCE(is_intra, false)) AS oi_amount_collectivite_self_inter,
  SUM(aom_other_amount) AS oi_amount_collectivite_other,
  SUM(ci_amount_self) AS ci_amount_total_self,
  SUM(ci_amount_other) AS ci_amount_total_other,
  SUM(ci_result_self) AS ci_result_total_self,
  SUM(ci_result_other) AS ci_result_total_other
FROM split
WHERE code IS NOT NULL
GROUP BY 1, {{ group_by_grain(grain, 2) }}

{% endmacro %}
