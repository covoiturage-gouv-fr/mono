{#
  verifie qu un siret de oi_details match (ou non) le siret de la colonne code_column
#}
{% macro territory_collectivite_siret_filter(code_column, match=true) %}
  elem ->> 'type' = 'collectivite' AND LEFT(elem ->> 'siret', 9) {{ '=' if match else '!=' }} {{ code_column }}
{% endmacro %}
