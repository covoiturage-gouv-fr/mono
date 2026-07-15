{% macro location_model(perim, grain) %}

  {# Histogramme H3 (heatmap de densité) pré-agrégé par territoire touché et période.
     Réplique la sémantique de l'ancienne requête à la volée de l'API : pour chaque
     territoire T où un trajet a un point (start_code = T OU end_code = T), on compte
     SES DEUX extrémités (start_h3index_z8 et end_h3index_z8). Le garde-fou
     `end_code IS DISTINCT FROM start_code` évite le double comptage des intra-T. #}

  {% set lookbacks = {
    'day':      {'nb': 3, 'unit': 'day'},
    'month':    {'nb': 1, 'unit': 'month'},
    'quarter':  {'nb': 1, 'unit': 'quarter'},
    'semester': {'nb': 1, 'unit': 'semester'},
    'year':     {'nb': 1, 'unit': 'year'}
  } %}

  {% if grain not in lookbacks %}
    {{ exceptions.raise_compiler_error("Invalid grain: " ~ grain) }}
  {% endif %}

  {% set lb = lookbacks[grain] %}

{% if perim == 'com' %}

{{ config(
  materialized='view',
  tags=['aggregated', 'location', grain, 'com']
) }}

SELECT * FROM {{ ref('location_' ~ grain ~ '_arr') }}
UNION ALL
SELECT * FROM {{ ref('location_' ~ grain ~ '_plm') }}

{% else %}

{{ config(
  materialized='incremental',
  incremental_strategy='delete+insert',
  unique_key=['territory', 'incremental_date', 'hex_z8'],
  indexes=[
    {'columns': ['territory', 'incremental_date']}
  ],
  tags=['aggregated', 'location', grain, perim, 'daily']
) }}

{% set plm_filter = "IN ('75056', '69123', '13055')" if perim == 'plm' else '' %}

WITH filtered_carpools AS (
  {{ filtered_carpools(perim, lookback_nb=lb.nb, lookback_unit=lb.unit) }}
),

-- Parité vue legacy : exclut les 2 derniers jours (données incomplètes).
recent AS (
  SELECT * FROM filtered_carpools WHERE carpool_datetime < now() - INTERVAL '2 day'
),

exploded AS (
  -- trajet rattaché par son départ au territoire start_code : ses 2 hexagones
  SELECT start_code AS territory, carpool_datetime, start_h3index_z8 AS hex_z8
  FROM recent WHERE start_code IS NOT NULL {% if plm_filter %}AND start_code {{ plm_filter }}{% endif %}
  UNION ALL
  SELECT start_code, carpool_datetime, end_h3index_z8
  FROM recent WHERE start_code IS NOT NULL {% if plm_filter %}AND start_code {{ plm_filter }}{% endif %}
  UNION ALL
  -- trajet rattaché par son arrivée à un territoire distinct : ses 2 hexagones
  SELECT end_code, carpool_datetime, start_h3index_z8
  FROM recent WHERE end_code IS NOT NULL AND end_code IS DISTINCT FROM start_code {% if plm_filter %}AND end_code {{ plm_filter }}{% endif %}
  UNION ALL
  SELECT end_code, carpool_datetime, end_h3index_z8
  FROM recent WHERE end_code IS NOT NULL AND end_code IS DISTINCT FROM start_code {% if plm_filter %}AND end_code {{ plm_filter }}{% endif %}
)

SELECT
  territory,
  {{ incremental_columns('carpool_datetime', grain) }},
  hex_z8,
  count(*)::bigint AS count
FROM exploded
GROUP BY 1, {{ group_by_grain(grain, 2) }}, hex_z8

{% endif %}
{% endmacro %}
