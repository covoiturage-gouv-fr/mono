{# Arrondissements de Paris, Lyon, Marseille -> commune, plus les codes communes eux-mêmes
   (géocodage parfois à la commune). #}
{% macro plm_perim() %}
  SELECT DISTINCT arr, com
  FROM {{ ref('perimeters') }}
  WHERE com IS NOT NULL AND arr <> com
  UNION VALUES ('75056','75056'), ('69123','69123'), ('13055','13055')
{% endmacro %}
