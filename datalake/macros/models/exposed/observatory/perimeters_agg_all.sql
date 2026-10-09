{# perimeters_agg + territoires custom (tenus à part : mis à jour chaque jour, pas au millésime) #}
{% macro perimeters_agg_all() %}
(
  SELECT year, code, type, libelle, geom, centroid FROM {{ ref('perimeters_agg') }}
  UNION ALL
  SELECT year, code, type, libelle, geom, centroid FROM {{ ref('custom_perimeters_agg') }}
)
{% endmacro %}
