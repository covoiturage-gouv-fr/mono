{#
  unique_drivers / unique_passengers d'un grain long de territory_* : COUNT(DISTINCT) sur les
  présences mensuelles (une personne active plusieurs mois ne compte qu'une fois).
  aomreg (both) : user_aomreg_month, sa règle dépend des deux extrémités du trajet.
  Autres : user_arr_month projeté sur le périmètre, avec les règles de filtered_carpools
  (aom strict exclut les AOM régionales, millésime de perimeters par date) ; from / to ne
  gardent que le côté start / end.
#}
{% macro rollup_uniques(perim, grain, direction) %}

  {% set lb = grain_lookback(grain, ['quarter', 'semester', 'year']) %}
  {% set side = {'from': "AND u.side = 'start'", 'to': "AND u.side = 'end'", 'both': ''}[direction] %}

  {% if perim == 'aomreg' and direction == 'both' %}
    {% set presence %}
      SELECT code, user_id, role, incremental_date FROM {{ ref('user_aomreg_month') }}
    {% endset %}
  {% elif perim == 'arr' %}
    {% set presence %}
      SELECT u.arr AS code, u.user_id, u.role, u.incremental_date
      FROM {{ ref('user_arr_month') }} AS u
      WHERE TRUE {{ side }}
    {% endset %}
  {% elif perim == 'plm' %}
    {% set presence %}
      SELECT p.com AS code, u.user_id, u.role, u.incremental_date
      FROM {{ ref('user_arr_month') }} AS u
      INNER JOIN ({{ plm_perim() }}) AS p ON p.arr = u.arr
      WHERE TRUE {{ side }}
    {% endset %}
  {% elif perim in ['epci', 'dep', 'reg', 'country', 'aom'] %}
    {% set presence %}
      SELECT p.{{ perim }} AS code, u.user_id, u.role, u.incremental_date
      FROM {{ ref('user_arr_month') }} AS u
      INNER JOIN {{ ref('perimeters') }} AS p
        ON p.arr = u.arr
        AND u.incremental_date >= p.valid_from AND u.incremental_date < p.valid_until
      WHERE p.{{ perim }} IS NOT NULL {{ side }}
      {% if perim == 'aom' %}
        AND p.aom NOT IN (SELECT aom FROM {{ ref('aom_region') }})
      {% endif %}
    {% endset %}
  {% else %}
    {{ exceptions.raise_compiler_error("rollup_uniques : périmètre non géré " ~ perim ~ " / " ~ direction) }}
  {% endif %}

  SELECT
    code,
    {{ incremental_columns('incremental_date', grain) }},
    COUNT(DISTINCT user_id) FILTER (WHERE role = 'driver') AS unique_drivers,
    COUNT(DISTINCT user_id) FILTER (WHERE role = 'passenger') AS unique_passengers
  FROM ({{ presence }}) AS presence
  WHERE {{ time_filter('incremental_date', type='timestamp', default_start="'2020-01-01 00:00:00'", lookback_nb=lb.nb, lookback_unit=lb.unit) }}
  GROUP BY 1, {{ group_by_grain(grain, 2) }}

{% endmacro %}
