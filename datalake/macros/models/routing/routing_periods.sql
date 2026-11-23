{# Périodes de la carte des flux pour la fenêtre [routing_from, routing_to) :
   un mois par mois de la fenêtre (month renseigné), une année par année
   entièrement couverte (month NULL). Une année partielle n'est jamais écrite :
   elle remplacerait l'année complète par quelques mois. #}
{% macro routing_periods() %}
  {% set start = var('routing_from') %}
  {% set end = var('routing_to') %}
  {% if start[8:] != '01' or end[8:] != '01' or start >= end %}
    {{ exceptions.raise_compiler_error(
      "routing_from / routing_to doivent être des débuts de mois, from < to : " ~ start ~ " / " ~ end
    ) }}
  {% endif %}
  SELECT
    extract(YEAR FROM m)::int  AS year,  -- noqa: RF04
    extract(MONTH FROM m)::int AS month  -- noqa: RF04
  FROM generate_series(
    '{{ start }}'::date, '{{ end }}'::date - interval '1 month', interval '1 month'
  ) AS m
  UNION ALL
  SELECT
    y         AS year,  -- noqa: RF04
    NULL::int AS month  -- noqa: RF04
  FROM generate_series(
    extract(YEAR FROM '{{ start }}'::date)::int, extract(YEAR FROM '{{ end }}'::date)::int
  ) AS y
  WHERE make_date(y, 1, 1) >= '{{ start }}'::date AND make_date(y + 1, 1, 1) <= '{{ end }}'::date
{% endmacro %}
