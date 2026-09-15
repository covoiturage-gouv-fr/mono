{#
  Montant total des incitations collectivite d une ligne carpool dont le SIREN
  matche ou non le code territoire courant.
#}
{% macro territory_oi_collectivite_amount(code_column, match=true) %}
  (
    SELECT COALESCE(SUM((elem ->> 'amount')::numeric), 0)
    FROM jsonb_array_elements(COALESCE(oi_details, '[]'::jsonb)) elem
    WHERE {{ territory_collectivite_siret_filter(code_column, match) }}
  )
{% endmacro %}
