{% macro territory_incentive_split_columns() %}
  {# Pour les aoms, on distingue ce qui est incite par l aom proprietaire, des autres aoms.
     oi_collectivite_*_amount/exists et ci_*_self/other sont precalcules une seule fois par
     ligne dans le split CTE (cf. territory_incentive_split_lateral), on ne fait ici que les
     agreger. #}
  SUM(oi_collectivite_self_amount) AS oi_amount_collectivite_self,
  SUM(oi_collectivite_self_amount)
    FILTER (WHERE is_intra) AS oi_amount_collectivite_self_intra,
  SUM(oi_collectivite_self_amount)
    FILTER (WHERE NOT is_intra) AS oi_amount_collectivite_self_inter,
  SUM(oi_collectivite_other_amount) AS oi_amount_collectivite_other,
  SUM(ci_amount_self) AS ci_amount_total_self,
  SUM(ci_amount_other) AS ci_amount_total_other,
  SUM(ci_result_self) AS ci_result_total_self,
  SUM(ci_result_other) AS ci_result_total_other,
  {# un trajet peut avoir plusieurs lignes d incitations, on ne le compte qu une fois #}
  COUNT(*) FILTER (WHERE oi_collectivite_self_exists) AS carpools_collectivite_self_incentive,
  COUNT(*) FILTER (WHERE oi_collectivite_other_exists) AS carpools_collectivite_other_incentive,
  {# un carpool peut avoir plusieurs lignes d incitation operateur/other, on ne le compte qu une fois #}
  COUNT(*) FILTER (WHERE oi_operator > 0) AS carpools_operator_incentive,
  COUNT(*) FILTER (WHERE oi_other > 0) AS carpools_other_incentive
{% endmacro %}
