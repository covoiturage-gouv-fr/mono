{{ config(
  materialized='view',
  tags=['app_metabase', 'aom_monthly_carpools']
) }}

SELECT
  'aom'                                                      AS perim,
  code,
  to_char(incremental_date, 'YYYY-MM')                       AS date,
  carpools,
  carpools
  - no_oi
    AS carpools_with_incentive,
  round(100.0 * (carpools - no_oi) / nullif(carpools, 0), 1) AS incited_pct,
  carpools_operator_incentive
    AS carpools_with_operator_incentive,
  oi_operator_only
    AS carpools_with_operator_incentive_only,
  carpools_collectivite_self_incentive
    AS carpools_with_aom_incentive,
  carpools_collectivite_other_incentive
    AS carpools_with_other_aom_incentive,
  carpools_other_incentive
    AS carpools_with_other_incentive,
  intra_carpools,
  carpools - intra_carpools                                  AS inter_carpools,
  q1_distance,
  median_distance,
  q3_distance
FROM {{ ref('territory_month_aom_both') }}
UNION ALL
SELECT
  'aomreg'                                                   AS perim,
  code,
  to_char(incremental_date, 'YYYY-MM')                       AS date,
  carpools,
  carpools
  - no_oi
    AS carpools_with_incentive,
  round(100.0 * (carpools - no_oi) / nullif(carpools, 0), 1) AS incited_pct,
  carpools_operator_incentive
    AS carpools_with_operator_incentive,
  oi_operator_only
    AS carpools_with_operator_incentive_only,
  carpools_collectivite_self_incentive
    AS carpools_with_aom_incentive,
  carpools_collectivite_other_incentive
    AS carpools_with_other_aom_incentive,
  carpools_other_incentive
    AS carpools_with_other_incentive,
  intra_carpools,
  carpools - intra_carpools                                  AS inter_carpools,
  q1_distance,
  median_distance,
  q3_distance
FROM {{ ref('territory_month_aomreg_both') }}
