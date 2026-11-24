{# Périodes de la carte des flux pour la fenêtre [routing_from, routing_to) :
   un mois par mois de la fenêtre (month renseigné), une année par année
   entièrement couverte (month NULL). Une année partielle n'est jamais écrite :
   elle remplacerait l'année complète par quelques mois. #}
{% macro routing_periods() %}
  {# string : une var YAML non quotée arrive en date ; la regex garde aussi
     l'interpolation SQL ci-dessous. #}
  {% set start = var('routing_from') | string %}
  {% set end = var('routing_to') | string %}
  {% set month_start = '^[0-9]{4}-[0-9]{2}-01$' %}
  {% if not modules.re.match(month_start, start) or not modules.re.match(month_start, end) or start >= end %}
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
