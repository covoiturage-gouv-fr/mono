-- territory_agg_column_names() pilote par nom le UNION ALL du cas 'com' (arr + plm), mais
-- est une liste statique recopiee a la main depuis les alias de territory_agg_columns() :
-- une divergence disparaitrait silencieusement du cas com (colonne manquante) ou casserait
-- la compilation (colonne inexistante). 
{{ config(severity='error', tags=['aggregated', 'territory']) }}

{% set actual_columns = adapter.get_columns_in_relation(ref('territory_day_arr_both')) | map(attribute='name') | list %}
{% set expected_columns = ['code', 'incremental_date'] + territory_agg_column_names() %}
{% set missing = expected_columns | reject('in', actual_columns) | list %}
{% set extra = actual_columns | reject('in', expected_columns) | list %}

{% if missing or extra %}
SELECT
  '{{ missing | join(", ") }}' AS missing_from_territory_agg_column_names,
  '{{ extra | join(", ") }}' AS extra_vs_territory_agg_columns
{% else %}
SELECT 1 AS failure WHERE false
{% endif %}
