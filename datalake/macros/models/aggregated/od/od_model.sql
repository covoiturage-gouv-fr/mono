{% macro od_model(perim, grain) %}

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

  {% set lb = lookbacks[grain] %}

{# --------------------------------------------------------
   Cas 'com' : vue UNION ALL des models arr + plm déjà calculés
-------------------------------------------------------- #}
{% if perim == 'com' %}

{{ config(
  materialized='view',
  tags=['aggregated', 'od', grain, 'com', 'daily']
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
  {% if perim == 'custom' %}
  {#- Une ligne par territoire touché, l'autre côté est NULL : least/greatest en ferait un
      intra. Hors territoire = 'ext' (pas l'arr : un territory_id peut valoir un code INSEE,
      et NULL casserait la clé delete+insert). -#}
  COALESCE(start_code, end_code) AS territory_1,
  CASE WHEN is_intra THEN COALESCE(start_code, end_code) ELSE 'ext' END AS territory_2,
  {% else %}
  least(start_code, end_code) AS territory_1,
  greatest(start_code, end_code) AS territory_2,
  {% endif %}
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
