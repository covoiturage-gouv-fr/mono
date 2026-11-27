{#
  columns : expressions de colonnes brutes, identiques pour chaque perim.
#}
{% macro mb_union_perims(perims, model_suffix, columns) %}
  {% for perim in perims %}
  SELECT '{{ perim }}' AS perim, {{ columns | join(', ') }}
  FROM {{ ref('user_' ~ perim ~ '_' ~ model_suffix) }}
  {% if not loop.last %}UNION ALL{% endif %}
  {% endfor %}
{% endmacro %}

{# Les codes se recouvrent entre perims (SIREN d'EPCI AOM = code aom, SIREN de région =
   code aom et aomreg, INSEE dep '11' = reg '11') : les vues mb_* se filtrent toujours
   sur perim. Familles séparées pour garder mb_aom_* tel que lu par les dashboards AOM. #}
{% macro mb_territory_perims() %}
  {% do return(['epci', 'dep', 'reg', 'country']) %}
{% endmacro %}

{% macro mb_union_aom_aomreg(model_suffix, columns) %}
  {{ mb_union_perims(['aom', 'aomreg'], model_suffix, columns) }}
{% endmacro %}

{% macro mb_union_territory(model_suffix, columns) %}
  {{ mb_union_perims(mb_territory_perims(), model_suffix, columns) }}
{% endmacro %}
