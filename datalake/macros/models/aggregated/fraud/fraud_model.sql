{% macro fraud_model(perim, grain, direction) %}

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
  tags=['aggregated', 'fraud', grain, 'com', direction]
) }}

SELECT * FROM {{ ref('fraud_' ~ grain ~ '_arr_' ~ direction) }}
UNION ALL
SELECT * FROM {{ ref('fraud_' ~ grain ~ '_plm_' ~ direction) }}

{% else %}

{{ config(
  materialized='incremental',
  incremental_strategy='delete+insert',
  unique_key=['operator_id', 'code', 'incremental_date'],
  indexes=[
    {'columns': ['operator_id', 'code', 'incremental_date'], 'unique': true}
  ],
  tags=['aggregated', 'fraud', grain, perim, direction, 'daily']
) }}

WITH filtered_carpools AS (
  {{ filtered_carpools(perim, lookback_nb=lb.nb, lookback_unit=lb.unit, with_valid=false, strict=true) }}
)

{% if direction == 'from' %}

  SELECT
    operator_id,
    operator_name,
    start_code AS code,
    {{ incremental_columns('carpool_datetime', grain) }},
    {{ fraud_agg_columns() }}
  FROM filtered_carpools
  WHERE start_code IS NOT NULL
  GROUP BY 1, 2, 3, {{ group_by_grain(grain, 4) }}

{% elif direction == 'to' %}

  SELECT
    operator_id,
    operator_name,
    end_code AS code,
    {{ incremental_columns('carpool_datetime', grain) }},
    {{ fraud_agg_columns() }}
  FROM filtered_carpools
  WHERE end_code IS NOT NULL
  GROUP BY 1, 2, 3, {{ group_by_grain(grain, 4) }}

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
    operator_id,
    operator_name,
    code,
    {{ incremental_columns('carpool_datetime', grain) }},
    {{ fraud_agg_columns() }}
  FROM exploded
  WHERE code IS NOT NULL
  GROUP BY 1, 2, 3, {{ group_by_grain(grain, 4) }}
{% endif %}
{% endif %}
{% endmacro %}