{{ config(
  materialized='view',
  tags=['app_metabase', 'aom_carpools_month']
) }}

SELECT
  'aom'                                                      AS perim,
  code,
  to_char(incremental_date, 'YYYY-MM')                       AS date,
  carpools,
  carpools
  - no_oi
    AS carpools_incited,
  round(100.0 * (carpools - no_oi) / nullif(carpools, 0), 1) AS incited_pct,
  carpools_operator_incentive
    AS carpools_incited_operator,
  carpools_operator_incentive_only
    AS carpools_incited_operator_only,
  carpools_collectivite_self_incentive
    AS carpools_incited_aom,
  carpools_collectivite_other_incentive
    AS carpools_incited_aom_other,
  carpools_other_incentive
    AS carpools_incited_other,
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
    AS carpools_incited,
  round(100.0 * (carpools - no_oi) / nullif(carpools, 0), 1) AS incited_pct,
  carpools_operator_incentive
    AS carpools_incited_operator,
  carpools_operator_incentive_only
    AS carpools_incited_operator_only,
  carpools_collectivite_self_incentive
    AS carpools_incited_aom,
  carpools_collectivite_other_incentive
    AS carpools_incited_aom_other,
  carpools_other_incentive
    AS carpools_incited_other,
  intra_carpools,
  carpools - intra_carpools                                  AS inter_carpools,
  q1_distance,
  median_distance,
  q3_distance
FROM {{ ref('territory_month_aomreg_both') }}
ORDER BY date, perim, code
