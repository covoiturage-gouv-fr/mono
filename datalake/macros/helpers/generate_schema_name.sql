{% macro generate_schema_name(custom_schema_name, node) -%}
  {%- set default_schema = target.schema -%}
  {%- if default_schema -%}
    {{ default_schema }}
  {% elif default_schema == '' %}
    {{ custom_schema_name | trim }}
  {%- else -%}
    public
  {%- endif -%}
{%- endmacro %}