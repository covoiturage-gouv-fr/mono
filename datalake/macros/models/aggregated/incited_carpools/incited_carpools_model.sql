{#
  Hors territory_agg_columns : ces compteurs en sont sortis (migration 0006) pour ne pas
  alourdir toutes les tables territory_*. Quirk : carpools_operator_incentive et
  carpools_other_incentive sont dans la liste de 0006 ; seul le préfixe de table
  (hors territory\_%) les protège d'un rejeu de cette migration.
#}
{% macro incited_carpools_model(perim) %}
  {{ perimeter_model(
    'incited_carpools', perim, 'month', 'both',
    columns=incited_carpools_columns(),
    filtered_args={'strict': true, 'with_new_users': false},
    on_schema_change='append_new_columns'
  ) }}
{% endmacro %}

{% macro incited_carpools_columns() %}
  COUNT(*) FILTER (WHERE oi_collectivite > 0) AS carpools_collectivite_incentive,
  COUNT(*) FILTER (WHERE oi_operator > 0) AS carpools_operator_incentive,
  COUNT(*) FILTER (WHERE oi_operator > 0 AND oi_collectivite = 0 AND oi_other = 0) AS carpools_operator_only_incentive,
  COUNT(*) FILTER (WHERE oi_other > 0) AS carpools_other_incentive
{% endmacro %}
