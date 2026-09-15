{#
  Montant ('amount' ou 'result') des recalculs de la SE d une ligne carpool dont le SIREN
  matche ou non le code territoire courant. 
#}
{% macro territory_campaign_amount(code_column, field='amount', match=true) %}
  (
    SELECT COALESCE(SUM((elem ->> '{{ field }}')::numeric), 0)
    FROM jsonb_array_elements(COALESCE(campaigns, '[]'::jsonb)) elem
    WHERE LEFT(elem ->> 'siret', 9) {{ '=' if match else '!=' }} {{ code_column }}
  )
{% endmacro %}
