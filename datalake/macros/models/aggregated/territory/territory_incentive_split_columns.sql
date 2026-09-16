{% macro territory_incentive_split_columns(code_column) %}
  {# Pour les aoms, on distingue ce qui est incite par l aom proprietaire, des autres aoms.#}
  SUM(oi_collectivite_self_amount) AS oi_amount_collectivite_self,
  SUM(oi_collectivite_self_amount)
    FILTER (WHERE is_intra) AS oi_amount_collectivite_self_intra,
  SUM(oi_collectivite_self_amount)
    FILTER (WHERE NOT is_intra) AS oi_amount_collectivite_self_inter,
  SUM(oi_collectivite_other_amount) AS oi_amount_collectivite_other,
  SUM({{ territory_campaign_amount(code_column, 'amount', true) }}) AS ci_amount_total_self,
  SUM({{ territory_campaign_amount(code_column, 'amount', false) }}) AS ci_amount_total_other,
  SUM({{ territory_campaign_amount(code_column, 'result', true) }}) AS ci_result_total_self,
  SUM({{ territory_campaign_amount(code_column, 'result', false) }}) AS ci_result_total_other,
  {# un trajet peut avoir plusieurs lignes d incitations, on ne le compte qu une fois #}
  COUNT(*) FILTER (
    WHERE EXISTS (
      SELECT 1 FROM jsonb_array_elements(COALESCE(oi_details, '[]'::jsonb)) elem
      WHERE {{ territory_collectivite_siret_filter(code_column, true) }}
    )
  ) AS carpools_collectivite_self_incentive,
  COUNT(*) FILTER (
    WHERE EXISTS (
      SELECT 1 FROM jsonb_array_elements(COALESCE(oi_details, '[]'::jsonb)) elem
      WHERE {{ territory_collectivite_siret_filter(code_column, false) }}
    )
  ) AS carpools_collectivite_other_incentive,
  {# un carpool peut avoir plusieurs lignes d incitation operateur/other, on ne le compte qu une fois #}
  COUNT(*) FILTER (WHERE oi_operator > 0) AS carpools_operator_incentive,
  COUNT(*) FILTER (WHERE oi_other > 0) AS carpools_other_incentive
{% endmacro %}
