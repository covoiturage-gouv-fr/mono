{#
  Grains longs (quarter, semester, year) agrégés depuis le modèle mensuel au lieu de relire
  les trajets sur toute la période (~80 % du pipeline-daily).

  Toutes les colonnes du mois se somment : un trajet appartient à un seul mois, et new_*
  compte le premier trajet global d'un utilisateur. Quirk : trips (COUNT DISTINCT
  operator_trip_id) compte deux fois un trajet dont les passagers sont à cheval sur deux mois.

  unique_drivers / unique_passengers ne se somment pas : lus dans `uniques` (SQL de
  rollup_uniques, joint sur code + incremental_date), sinon absents (migration 0007).
#}
{% macro rollup_model(month_relation, grain, keys, uniques=none) %}

  {% set lb = grain_lookback(grain, ['quarter', 'semester', 'year']) %}
  {% set period_cols = ['incremental_date', 'year', 'month'] %}
  {% set distinct_cols = ['unique_drivers', 'unique_passengers'] %}
  {% set array_lengths = {
    'hours_distribution': 24,
    'dist_distribution': dist_classes() | length,
    'oc_distribution': 3
  } %}
  {% set columns = adapter.get_columns_in_relation(month_relation) if execute else [] %}
  {% set names = columns | map(attribute='name') | list %}

WITH months AS (
  SELECT *
  FROM {{ month_relation }}
  WHERE {{ time_filter('incremental_date', type='timestamp', default_start="'2020-01-01 00:00:00'", lookback_nb=lb.nb, lookback_unit=lb.unit) }}
),

{% if uniques %}
uniques AS (
  {{ uniques }}
),
{% endif %}

rolled AS (
  SELECT
    {% for k in keys %}{{ k }},{% endfor %}
    {{ incremental_columns('incremental_date', grain) }}
    {% for c in columns if c.name not in keys + period_cols + distinct_cols %}
      {% if c.data_type == 'ARRAY' %}
        {% if c.name not in array_lengths %}
          {{ exceptions.raise_compiler_error("rollup_model : longueur inconnue pour le tableau " ~ c.name) }}
        {% endif %}
    , ARRAY[
      {% for i in range(array_lengths[c.name]) %}
        SUM({{ c.name }}[{{ i + 1 }}]){% if not loop.last %},{% endif %}
      {% endfor %}
    ]::bigint[] AS {{ c.name }}
      {% else %}
    , SUM({{ c.name }})::{{ c.data_type }} AS {{ c.name }}
      {% endif %}
    {% endfor %}
  FROM months
  GROUP BY {{ range(1, keys | length + 1) | join(', ') }}{% if keys %}, {% endif %}{{ group_by_grain(grain, keys | length + 1) }}
)

SELECT
  r.*
  {% if uniques %}
    {% for d in distinct_cols if d in names %}
  , u.{{ d }}
    {% endfor %}
  {% endif %}
FROM rolled AS r
{% if uniques %}
LEFT JOIN uniques AS u ON u.code = r.code AND u.incremental_date = r.incremental_date
{% endif %}

{% endmacro %}
