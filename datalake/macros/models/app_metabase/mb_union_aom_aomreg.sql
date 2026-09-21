{#
  Union 'aom' + 'aomreg' d'un même modèle user_<perim>_<model_suffix>, perim en
  colonne littérale. columns est la liste des expressions de colonnes (brutes,
  identiques des deux côtés) à sélectionner.
#}
{% macro mb_union_aom_aomreg(model_suffix, columns) %}
  SELECT 'aom' AS perim, {{ columns | join(', ') }}
  FROM {{ ref('user_aom_' ~ model_suffix) }}
  UNION ALL
  SELECT 'aomreg' AS perim, {{ columns | join(', ') }}
  FROM {{ ref('user_aomreg_' ~ model_suffix) }}
{% endmacro %}
