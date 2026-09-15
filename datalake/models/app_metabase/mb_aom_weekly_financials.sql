{{ config(
  materialized='view',
  tags=['app_metabase', 'aom_weekly_financials']
) }}

WITH daily AS (
  SELECT
    'aom'                     AS perim,
    code,
    incremental_date,
    carpools,
    intra_carpools,
    carpools - intra_carpools AS inter_carpools,
    driver_revenue,
    driver_revenue_intra,
    driver_revenue_inter,
    passenger_contribution,
    passenger_contribution_intra,
    passenger_contribution_inter,
    oi_amount_total,
    oi_amount_total_intra,
    oi_amount_total_inter,
    oi_amount_collectivite_self,
    oi_amount_collectivite_self_intra,
    oi_amount_collectivite_self_inter,
    oi_amount_collectivite_other,
    oi_amount_operator,
    oi_amount_operator_intra,
    oi_amount_operator_inter,
    ci_amount_total_self,
    ci_amount_total_other,
    ci_result_total_self,
    ci_result_total_other
  FROM {{ ref('territory_day_aom_both') }}
  UNION ALL
  SELECT
    'aomreg'                  AS perim,
    code,
    incremental_date,
    carpools,
    intra_carpools,
    carpools - intra_carpools AS inter_carpools,
    driver_revenue,
    driver_revenue_intra,
    driver_revenue_inter,
    passenger_contribution,
    passenger_contribution_intra,
    passenger_contribution_inter,
    oi_amount_total,
    oi_amount_total_intra,
    oi_amount_total_inter,
    oi_amount_collectivite_self,
    oi_amount_collectivite_self_intra,
    oi_amount_collectivite_self_inter,
    oi_amount_collectivite_other,
    oi_amount_operator,
    oi_amount_operator_intra,
    oi_amount_operator_inter,
    ci_amount_total_self,
    ci_amount_total_other,
    ci_result_total_self,
    ci_result_total_other
  FROM {{ ref('territory_day_aomreg_both') }}
),

weekly AS (
  SELECT
    perim,
    code,
    date_trunc('week', incremental_date)::date AS week,
    sum(carpools)                              AS carpools,
    sum(intra_carpools)                        AS intra_carpools,
    sum(inter_carpools)                        AS inter_carpools,
    sum(driver_revenue)                        AS driver_revenue,
    sum(driver_revenue_intra)                  AS driver_revenue_intra,
    sum(driver_revenue_inter)                  AS driver_revenue_inter,
    sum(passenger_contribution)                AS passenger_contribution,
    sum(passenger_contribution_intra)          AS passenger_contribution_intra,
    sum(passenger_contribution_inter)          AS passenger_contribution_inter,
    sum(oi_amount_total)                       AS oi_amount_total,
    sum(oi_amount_total_intra)                 AS oi_amount_total_intra,
    sum(oi_amount_total_inter)                 AS oi_amount_total_inter,
    sum(oi_amount_collectivite_self)           AS oi_amount_collectivite_self,
    sum(oi_amount_collectivite_self_intra)     AS oi_amount_collectivite_self_intra,
    sum(oi_amount_collectivite_self_inter)     AS oi_amount_collectivite_self_inter,
    sum(oi_amount_collectivite_other)          AS oi_amount_collectivite_other,
    sum(oi_amount_operator)                    AS oi_amount_operator,
    sum(oi_amount_operator_intra)              AS oi_amount_operator_intra,
    sum(oi_amount_operator_inter)              AS oi_amount_operator_inter,
    sum(ci_amount_total_self)                  AS ci_amount_total_self,
    sum(ci_amount_total_other)                 AS ci_amount_total_other,
    sum(ci_result_total_self)                  AS ci_result_total_self,
    sum(ci_result_total_other)                 AS ci_result_total_other
  FROM daily
  GROUP BY perim, code, date_trunc('week', incremental_date)
)

SELECT
  perim,
  code,
  week,
  carpools,
  intra_carpools,
  inter_carpools,
  round(driver_revenue::numeric / nullif(carpools, 0), 2)
    AS avg_driver_revenue,
  round(driver_revenue_intra::numeric / nullif(intra_carpools, 0), 2)
    AS avg_driver_revenue_intra,
  round(driver_revenue_inter::numeric / nullif(inter_carpools, 0), 2)
    AS avg_driver_revenue_inter,
  round(passenger_contribution::numeric / nullif(carpools, 0), 2)
    AS avg_passenger_contribution,
  round(passenger_contribution_intra::numeric / nullif(intra_carpools, 0), 2)
    AS avg_passenger_contribution_intra,
  round(passenger_contribution_inter::numeric / nullif(inter_carpools, 0), 2)
    AS avg_passenger_contribution_inter,
  round(oi_amount_total / nullif(carpools, 0), 2)
    AS avg_incentive,
  round(oi_amount_total_intra / nullif(intra_carpools, 0), 2)
    AS avg_incentive_intra,
  round(oi_amount_total_inter / nullif(inter_carpools, 0), 2)
    AS avg_incentive_inter,
  round(oi_amount_collectivite_self / nullif(carpools, 0), 2)
    AS avg_incentive_aom,
  round(oi_amount_collectivite_self_intra / nullif(intra_carpools, 0), 2)
    AS avg_incentive_aom_intra,
  round(oi_amount_collectivite_self_inter / nullif(inter_carpools, 0), 2)
    AS avg_incentive_aom_inter,
  round(oi_amount_operator / nullif(carpools, 0), 2)
    AS avg_incentive_operator,
  round(oi_amount_operator_intra / nullif(intra_carpools, 0), 2)
    AS avg_incentive_operator_intra,
  round(oi_amount_operator_inter / nullif(inter_carpools, 0), 2)
    AS avg_incentive_operator_inter,
  oi_amount_collectivite_self,
  oi_amount_collectivite_other,
  ci_amount_total_self,
  ci_amount_total_other,
  ci_result_total_self,
  ci_result_total_other,
  oi_amount_collectivite_self - ci_result_total_self
    AS diff_op_rpc,
  round(
    100.0 * (oi_amount_collectivite_self - ci_result_total_self)
    / nullif(ci_result_total_self, 0), 1
  ) AS diff_op_rpc_pct
FROM weekly
ORDER BY week, perim, code
