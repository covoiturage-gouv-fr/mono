{% macro od_model(perim, grain) %}

  {# --------------------------------------------------------
     Lookback par grain
  -------------------------------------------------------- #}
  {% set lookbacks = {
    'day':      {'nb': 3, 'unit': 'day'},
    'month':    {'nb': 1, 'unit': 'month'},
    'quarter':  {'nb': 1, 'unit': 'quarter'},
    'semester': {'nb': 0, 'unit': 'semester'},
    'year':     {'nb': 0, 'unit': 'year'}
  } %}

  {% if grain not in lookbacks %}
    {{ exceptions.raise_compiler_error("Invalid grain: " ~ grain ~ ". Expected: day, month, quarter, semester, year") }}
  {% endif %}

  {% set lb = lookbacks[grain] %}

{# --------------------------------------------------------
   Cas 'com' : vue UNION ALL des models arr + plm déjà calculés
-------------------------------------------------------- #}
{% if perim == 'com' %}

{{ config(
  materialized='view',
  tags=['aggregated', 'od', grain, 'com']
) }}

SELECT * FROM {{ ref('od_' ~ grain ~ '_arr') }}
UNION ALL
SELECT * FROM {{ ref('od_' ~ grain ~ '_plm') }}

{% else %}

{{ config(
  materialized='incremental',
  incremental_strategy='delete+insert',
  unique_key=['territory_1', 'territory_2', 'incremental_date'],
  indexes=[
    {'columns': ['territory_1', 'territory_2', 'incremental_date'], 'unique': true}
  ],
  tags=['aggregated', 'od', grain, perim, 'daily']
) }}

WITH filtered_carpools AS (
  {{ filtered_carpools(perim, lookback_nb=lb.nb, lookback_unit=lb.unit) }}
)

SELECT
  least(start_code, end_code) AS territory_1,
  greatest(start_code, end_code) AS territory_2,
  {{ incremental_columns('carpool_datetime', grain) }},
  {{ od_agg_columns() }}
FROM filtered_carpools
{% if perim == 'plm' %}
WHERE start_code IN ('75056', '69123', '13055')
  OR end_code IN ('75056', '69123', '13055')
{% endif %}
GROUP BY 1, 2, {{ group_by_grain(grain, 3) }}

{% endif %}
{% endmacro %}
