-- Garde-fou : les compteurs de trajets incités sont des sous-ensembles de
-- carpools. Un dépassement trahit un SUM sur les oi_* (lignes d'incitation)
-- au lieu d'un COUNT FILTER > 0. Compteurs NULL : mois jamais construit dans
-- incited_carpools_month_* (ex. avant 2020 sans backfill). Ne détecte pas les
-- lignes fantômes : même delete+insert des deux côtés, elles s'y retrouvent.
{{ config(severity='warn', tags=['app_metabase', 'territory_monthly_carpools']) }}

SELECT *
FROM {{ ref('mb_territory_monthly_carpools') }}
WHERE
  carpools_with_incentive > carpools
  OR carpools_with_incentive < 0
  OR carpools_with_collectivite_incentive > carpools_with_incentive
  OR carpools_with_operator_incentive > carpools_with_incentive
  OR carpools_with_other_incentive > carpools_with_incentive
  OR carpools_with_operator_incentive_only > carpools_with_operator_incentive
  OR inter_carpools < 0
  OR (
    carpools > 0
    AND (
      carpools_with_collectivite_incentive IS NULL
      OR carpools_with_operator_incentive IS NULL
      OR carpools_with_operator_incentive_only IS NULL
      OR carpools_with_other_incentive IS NULL
    )
  )
