{#
  Squelette commun des agrégats par périmètre et direction (territory, fraud, operators).

  - kind : préfixe des modèles et tag (territory_<grain>_<perim>_<direction>)
  - columns : SQL des colonnes agrégées (sortie d'une macro *_agg_columns)
  - keys / key_ids : colonnes ajoutées avant code (SELECT / GROUP BY) et celles de la clé unique
  - filtered_args : options passées à filtered_carpools
  - on_schema_change : modèles incrémentaux seulement, omis s'il vaut none (défaut du projet)

  Cas com : vue UNION ALL de arr + plm. Les deux tables sont altérées indépendamment
  (on_schema_change) et leur ordre physique de colonnes peut diverger : le UNION ALL énumère
  les colonnes de arr par nom plutôt que SELECT *. Une colonne absente de plm casse la vue.

  direction 'both' : un trajet est compté pour son départ, et pour son arrivée s'il n'est pas intra.
#}
{% macro perimeter_model(
  kind,
  perim,
  grain,
  direction,
  columns,
  keys=[],
  key_ids=[],
  filtered_args={},
  on_schema_change=none
) %}

  {% set lb = grain_lookback(grain) %}

  {% if direction not in ['from', 'to', 'both'] %}
    {{ exceptions.raise_compiler_error("Invalid direction: " ~ direction ~ ". Expected: from, to, both") }}
  {% endif %}

  {% set schema_change = {'on_schema_change': on_schema_change} if on_schema_change else {} %}
  {% set group_by = range(1, keys | length + 2) | join(', ') ~ ', ' ~ group_by_grain(grain, keys | length + 2) %}

{% if perim == 'com' %}

{{ config(
  materialized='view',
  tags=['aggregated', kind, grain, 'com', direction, 'daily']
) }}

{% set arr = ref(kind ~ '_' ~ grain ~ '_arr_' ~ direction) %}
{% set com_columns = adapter.get_columns_in_relation(arr) | map(attribute='name') | list if execute else [] %}
{% set select_list = com_columns | join(', ') if com_columns else '*' %}

SELECT {{ select_list }} FROM {{ arr }}
UNION ALL
SELECT {{ select_list }} FROM {{ ref(kind ~ '_' ~ grain ~ '_plm_' ~ direction) }}

{% else %}

{{ config(
  materialized='incremental',
  incremental_strategy='delete+insert',
  unique_key=key_ids + ['code', 'incremental_date'],
  indexes=[
    {'columns': key_ids + ['code', 'incremental_date'], 'unique': true}
  ],
  tags=['aggregated', kind, grain, perim, direction, 'daily'],
  **schema_change
) }}

WITH filtered_carpools AS (
  {{ filtered_carpools(perim, lookback_nb=lb.nb, lookback_unit=lb.unit, **filtered_args) }}
)

{% if direction == 'from' %}

  SELECT
    {% for k in keys %}{{ k }},{% endfor %}
    start_code AS code,
    {{ incremental_columns('carpool_datetime', grain) }},
    {{ columns }}
  FROM filtered_carpools
  WHERE start_code IS NOT NULL
  GROUP BY {{ group_by }}

{% elif direction == 'to' %}

  SELECT
    {% for k in keys %}{{ k }},{% endfor %}
    end_code AS code,
    {{ incremental_columns('carpool_datetime', grain) }},
    {{ columns }}
  FROM filtered_carpools
  WHERE end_code IS NOT NULL
  GROUP BY {{ group_by }}

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
    {% for k in keys %}{{ k }},{% endfor %}
    code,
    {{ incremental_columns('carpool_datetime', grain) }},
    {{ columns }}
  FROM exploded
  WHERE code IS NOT NULL
  GROUP BY {{ group_by }}
{% endif %}
{% endif %}
{% endmacro %}
