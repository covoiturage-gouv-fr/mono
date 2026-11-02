{% macro territory_model(perim, grain, direction) %}

  {# --------------------------------------------------------
     Lookback par grain
  -------------------------------------------------------- #}
  {% set lookbacks = {
    'day':      {'nb': 3, 'unit': 'day'},
    'month':    {'nb': 1, 'unit': 'month'},
    'quarter':  {'nb': 1, 'unit': 'quarter'},
    'semester': {'nb': 1, 'unit': 'semester'},
    'year':     {'nb': 1, 'unit': 'year'}
  } %}

  {% if grain not in lookbacks %}
    {{ exceptions.raise_compiler_error("Invalid grain: " ~ grain ~ ". Expected: day, month, quarter, semester, year") }}
  {% endif %}

  {% if direction not in ['from', 'to', 'both'] %}
    {{ exceptions.raise_compiler_error("Invalid direction: " ~ direction ~ ". Expected: from, to, both") }}
  {% endif %}

  {% set lb = lookbacks[grain] %}


{# --------------------------------------------------------
   Cas 'com' : vue UNION ALL des models arr + plm déjà calculés
-------------------------------------------------------- #}
{% if perim == 'com' %}

{{ config(
  materialized='view',
  tags=['aggregated', 'territory', grain, 'com', direction, 'daily'],
  on_schema_change='append_new_columns'
) }}

{# arr et plm sont full-refreshés/altérés indépendamment : leur ordre physique de colonnes
   peut diverger. On énumère les colonnes par nom pour que le UNION ALL apparie par nom,
   pas par position (cf. territory_agg_column_names). #}
{% set incremental_col_names = {
  'day':      ['incremental_date'],
  'month':    ['incremental_date', 'year', 'month'],
  'quarter':  ['incremental_date', 'year', 'quarter'],
  'semester': ['incremental_date', 'year', 'semester'],
  'year':     ['incremental_date', 'year']
} %}
{% set com_columns = ['code'] + incremental_col_names[grain] + territory_agg_column_names() %}

SELECT {{ com_columns | join(', ') }} FROM {{ ref('territory_' ~ grain ~ '_arr_' ~ direction) }}
UNION ALL
SELECT {{ com_columns | join(', ') }} FROM {{ ref('territory_' ~ grain ~ '_plm_' ~ direction) }}

{% else %}

{{ config(
  materialized='incremental',
  incremental_strategy='delete+insert',
  unique_key=['code', 'incremental_date'],
  on_schema_change='append_new_columns',
  indexes=[
    {'columns': ['code', 'incremental_date'], 'unique': true}
  ],
  tags=['aggregated', 'territory', grain, perim, direction, 'daily']
) }}

WITH filtered_carpools AS (
  {{ filtered_carpools(perim, lookback_nb=lb.nb, lookback_unit=lb.unit, strict=true) }}
)

{% if direction == 'from' %}

  SELECT
    start_code AS code,
    {{ incremental_columns('carpool_datetime', grain) }},
    {{ territory_agg_columns() }}
  FROM filtered_carpools
  WHERE start_code IS NOT NULL
  GROUP BY 1, {{ group_by_grain(grain, 2) }}

{% elif direction == 'to' %}

  SELECT
    end_code AS code,
    {{ incremental_columns('carpool_datetime', grain) }},
    {{ territory_agg_columns() }}
  FROM filtered_carpools
  WHERE end_code IS NOT NULL
  GROUP BY 1, {{ group_by_grain(grain, 2) }}

{% elif direction == 'both' %}
  , exploded AS (
    SELECT *, start_code AS code
    FROM filtered_carpools
    WHERE start_code IS NOT NULL
    UNION ALL
    SELECT *, end_code AS code
    FROM filtered_carpools
    WHERE end_code IS NOT NULL
    AND NOT is_intra
  )
  SELECT
    code,
    {{ incremental_columns('carpool_datetime', grain) }},
    {{ territory_agg_columns() }}
  FROM exploded
  WHERE code IS NOT NULL
  GROUP BY 1, {{ group_by_grain(grain, 2) }}
{% endif %}
{% endif %}
{% endmacro %}