{% macro user_territory_model(perim, grain, direction) %}

  {% set lookbacks = {
    'day':   {'nb': 3, 'unit': 'day'},
    'month': {'nb': 1, 'unit': 'month'}
  } %}

  {% if grain not in lookbacks %}
    {{ exceptions.raise_compiler_error("Invalid grain: " ~ grain ~ ". Expected: day, month") }}
  {% endif %}

  {% if perim not in ['aom', 'aomreg'] %}
    {{ exceptions.raise_compiler_error("Invalid perim: " ~ perim ~ ". Expected: aom, aomreg") }}
  {% endif %}

  {% if direction not in ['from', 'to', 'both'] %}
    {{ exceptions.raise_compiler_error("Invalid direction: " ~ direction ~ ". Expected: from, to, both") }}
  {% endif %}

  {% set lb = lookbacks[grain] %}

{{ config(
  materialized='incremental',
  incremental_strategy='delete+insert',
  unique_key=['user_id', 'role', 'code', 'incremental_date'],
  indexes=[
    {'columns': ['user_id', 'role', 'code', 'incremental_date'], 'unique': true}
  ],
  tags=['aggregated', 'users', grain, perim, direction, 'daily']
) }}

WITH filtered_carpools AS (
  {{ filtered_carpools(perim, lookback_nb=lb.nb, lookback_unit=lb.unit, with_new_users=false, strict=true) }}
)

{% if direction == 'from' %}
, coded AS (
  SELECT *, start_code AS code
  FROM filtered_carpools
  WHERE start_code IS NOT NULL
)
{% elif direction == 'to' %}
, coded AS (
  SELECT *, end_code AS code
  FROM filtered_carpools
  WHERE end_code IS NOT NULL
)
{% elif direction == 'both' %}
{# code = start_code ou end_code : un trajet intra n'est compté qu'une fois (via
   start_code), comme territory_model direction='both'. #}
, coded AS (
  SELECT *, start_code AS code
  FROM filtered_carpools
  WHERE start_code IS NOT NULL
  UNION ALL
  SELECT *, end_code AS code
  FROM filtered_carpools
  WHERE end_code IS NOT NULL
    AND NOT is_intra
)
{% endif %}

, carpools_by_role AS (
  SELECT
    driver_key     AS user_id,
    'driver'::text AS role,  -- noqa: RF04
    *
  FROM coded
  WHERE driver_key IS NOT NULL
  UNION ALL
  SELECT
    passenger_key     AS user_id,
    'passenger'::text AS role,  -- noqa: RF04
    *
  FROM coded
  WHERE passenger_key IS NOT NULL
)

SELECT
  user_id,
  role,  -- noqa: RF04
  code,
  {{ incremental_columns('carpool_datetime', grain) }},
  {{ user_agg_columns() }}
FROM carpools_by_role
WHERE code IS NOT NULL
GROUP BY 1, 2, 3, {{ group_by_grain(grain, 4) }}

{% endmacro %}
