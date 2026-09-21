{#
  verifie qu un siret de oi_details match (ou non) le siret de la colonne code_column.
  restreint aux collectivites de type aom (is_aom) : une collectivite type='collectivite'
  peut aussi etre un epci
#}
{% macro territory_collectivite_siret_filter(code_column, match=true) %}
  elem ->> 'type' = 'collectivite' AND (elem ->> 'is_aom')::boolean AND LEFT(elem ->> 'siret', 9) {{ '=' if match else '!=' }} {{ code_column }}
{% endmacro %}
