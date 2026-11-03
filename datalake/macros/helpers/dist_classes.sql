{# Tranches de distance de 5 km : (libellé, borne haute exclue en m), none pour la dernière.
   Source de carpools.dist_class et de l'ordre de dist_distribution (territory_agg_columns),
   dont distribution_exposed relit les indices. #}
{% macro dist_classes() %}
  {% set classes = [] %}
  {% for km in range(5, 85, 5) %}
    {% do classes.append(('%02d-%02d' | format(km - 5, km), km * 1000)) %}
  {% endfor %}
  {% do classes.append(('80+', none)) %}
  {{ return(classes) }}
{% endmacro %}
