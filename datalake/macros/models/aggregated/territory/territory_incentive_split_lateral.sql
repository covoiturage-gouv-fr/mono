{#
  Calcul en amont les incitations et recalcule selfs/others pour les aom
  évite multiples scans du jsonb array
#}
{% macro territory_incentive_split_lateral(code_column) %}
  LEFT JOIN LATERAL (
    SELECT
      COALESCE(SUM((elem ->> 'amount')::numeric) FILTER (
        WHERE {{ territory_collectivite_siret_filter(code_column, true) }}
      ), 0) AS oi_collectivite_self_amount,
      COALESCE(SUM((elem ->> 'amount')::numeric) FILTER (
        WHERE {{ territory_collectivite_siret_filter(code_column, false) }}
      ), 0) AS oi_collectivite_other_amount,
      COALESCE(bool_or({{ territory_collectivite_siret_filter(code_column, true) }}), false)
        AS oi_collectivite_self_exists,
      COALESCE(bool_or({{ territory_collectivite_siret_filter(code_column, false) }}), false)
        AS oi_collectivite_other_exists,
      COALESCE(bool_and(elem ->> 'type' = 'operator'), false) AS is_operator_only_incentive
    FROM jsonb_array_elements(COALESCE(oi_details, '[]'::jsonb)) elem
  ) oi_split ON true
  LEFT JOIN LATERAL (
    SELECT
      COALESCE(SUM((elem ->> 'amount')::numeric)
        FILTER (WHERE LEFT(elem ->> 'siret', 9) = {{ code_column }}), 0) AS ci_amount_self,
      COALESCE(SUM((elem ->> 'amount')::numeric)
        FILTER (WHERE LEFT(elem ->> 'siret', 9) != {{ code_column }}), 0) AS ci_amount_other,
      COALESCE(SUM((elem ->> 'result')::numeric)
        FILTER (WHERE LEFT(elem ->> 'siret', 9) = {{ code_column }}), 0) AS ci_result_self,
      COALESCE(SUM((elem ->> 'result')::numeric)
        FILTER (WHERE LEFT(elem ->> 'siret', 9) != {{ code_column }}), 0) AS ci_result_other
    FROM jsonb_array_elements(COALESCE(campaigns, '[]'::jsonb)) elem
  ) ci_split ON true
{% endmacro %}
