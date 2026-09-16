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

  {# cas ou le perim est une aom(r) ; on regarde si les incitations sont portées par l aom du perim ou alors une autre aom. #}
  {% set with_incentive_split = perim in ['aom', 'aomreg'] %}

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
  {{ filtered_carpools(perim, lookback_nb=lb.nb, lookback_unit=lb.unit, with_oi_details=with_incentive_split, strict=true) }}
)

{% if direction == 'from' %}

  {% if with_incentive_split %}
  , split AS (
    SELECT
      filtered_carpools.*,
      oi_split.oi_collectivite_self_amount,
      oi_split.oi_collectivite_other_amount,
      oi_split.oi_collectivite_self_exists,
      oi_split.oi_collectivite_other_exists,
      oi_split.is_operator_only_incentive,
      ci_split.ci_amount_self,
      ci_split.ci_amount_other,
      ci_split.ci_result_self,
      ci_split.ci_result_other
    FROM filtered_carpools
    {{ territory_incentive_split_lateral('start_code') }}
  )
  {% endif %}
  SELECT
    start_code AS code,
    {{ incremental_columns('carpool_datetime', grain) }},
    {{ territory_agg_columns(with_incentive_split) }}
    {% if with_incentive_split %}
    ,
    {{ territory_incentive_split_columns() }}
    {% endif %}
  FROM {{ 'split' if with_incentive_split else 'filtered_carpools' }}
  WHERE start_code IS NOT NULL
  GROUP BY 1, {{ group_by_grain(grain, 2) }}

{% elif direction == 'to' %}

  {% if with_incentive_split %}
  , split AS (
    SELECT
      filtered_carpools.*,
      oi_split.oi_collectivite_self_amount,
      oi_split.oi_collectivite_other_amount,
      oi_split.oi_collectivite_self_exists,
      oi_split.oi_collectivite_other_exists,
      oi_split.is_operator_only_incentive,
      ci_split.ci_amount_self,
      ci_split.ci_amount_other,
      ci_split.ci_result_self,
      ci_split.ci_result_other
    FROM filtered_carpools
    {{ territory_incentive_split_lateral('end_code') }}
  )
  {% endif %}
  SELECT
    end_code AS code,
    {{ incremental_columns('carpool_datetime', grain) }},
    {{ territory_agg_columns(with_incentive_split) }}
    {% if with_incentive_split %}
    ,
    {{ territory_incentive_split_columns() }}
    {% endif %}
  FROM {{ 'split' if with_incentive_split else 'filtered_carpools' }}
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
  {% if with_incentive_split %}
  , split AS (
    SELECT
      exploded.*,
      oi_split.oi_collectivite_self_amount,
      oi_split.oi_collectivite_other_amount,
      oi_split.oi_collectivite_self_exists,
      oi_split.oi_collectivite_other_exists,
      oi_split.is_operator_only_incentive,
      ci_split.ci_amount_self,
      ci_split.ci_amount_other,
      ci_split.ci_result_self,
      ci_split.ci_result_other
    FROM exploded
    {{ territory_incentive_split_lateral('code') }}
  )
  {% endif %}
  SELECT
    code,
    {{ incremental_columns('carpool_datetime', grain) }},
    {{ territory_agg_columns(with_incentive_split) }}
    {% if with_incentive_split %}
    ,
    {{ territory_incentive_split_columns() }}
    {% endif %}
  FROM {{ 'split' if with_incentive_split else 'exploded' }}
  WHERE code IS NOT NULL
  GROUP BY 1, {{ group_by_grain(grain, 2) }}
{% endif %}
{% endif %}
{% endmacro %}