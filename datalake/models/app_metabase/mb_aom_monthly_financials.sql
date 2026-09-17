{{ config(
  materialized='view',
  tags=['app_metabase', 'aom_monthly_financials']
) }}

WITH monthly AS (
  SELECT
    'aom'                                               AS perim,
    code,
    incremental_date,
    {{ mb_aom_financials_columns() | join(',\n    ') }}
  FROM {{ ref('territory_month_aom_both') }}
  UNION ALL
  SELECT
    'aomreg'                                            AS perim,
    code,
    incremental_date,
    {{ mb_aom_financials_columns() | join(',\n    ') }}
  FROM {{ ref('territory_month_aomreg_both') }}
)

SELECT
  perim,
  code,
  carpools,
  intra_carpools,
  inter_carpools,
  oi_amount_collectivite_self,
  oi_amount_collectivite_other,
  ci_amount_total_self,
  ci_amount_total_other,
  ci_result_total_self,
  ci_result_total_other,
  to_char(incremental_date, 'YYYY-MM')
    AS date,
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
  oi_amount_collectivite_self - ci_result_total_self
    AS diff_op_rpc,
  round(
    100.0 * (oi_amount_collectivite_self - ci_result_total_self)
    / nullif(ci_result_total_self, 0), 1
  )
    AS diff_op_rpc_pct
FROM monthly
