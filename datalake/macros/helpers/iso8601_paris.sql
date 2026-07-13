{% macro iso8601_paris(ts) %}
{#- Horodatage ISO 8601 « yyyy-MM-dd'T'HH:mm:ssXX » : champs locaux bruts + offset UTC d'Europe/Paris (DST, toujours positif UTC+1/+2). Reproduit toTzString de l'ancien export open-data. -#}
{%- set offset = "(" ~ ts ~ ") - ((" ~ ts ~ ") AT TIME ZONE 'Europe/Paris' AT TIME ZONE 'UTC')" -%}
to_char({{ ts }}, 'YYYY-MM-DD"T"HH24:MI:SS') || '+' || to_char({{ offset }}, 'HH24MI')
{% endmacro %}
