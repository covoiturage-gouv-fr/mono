{% macro delete_window(path_prefix, start, end, date_col='incremental_date') %}
  {% if execute %}
    {% set ns = namespace(count=0) %}
    {% for node in graph.nodes.values() %}
      {% if node.resource_type == 'model'
         and node.config.materialized == 'incremental'
         and node.original_file_path.startswith(path_prefix) %}
        {% set relation = adapter.get_relation(
            database=node.database,
            schema=node.schema,
            identifier=node.alias
        ) %}
        {% if relation %}
          {% set delete_sql %}
            DELETE FROM {{ relation }}
            WHERE {{ date_col }} >= '{{ start }}'::date
              AND {{ date_col }} < '{{ end }}'::date
          {% endset %}
          {% do run_query(delete_sql) %}
          {{ log("deleted: " ~ relation ~ " [" ~ start ~ " → " ~ end ~ "]", info=True) }}
          {% set ns.count = ns.count + 1 %}
        {% endif %}
      {% endif %}
    {% endfor %}
    {{ log(ns.count ~ " table(s) nettoyées", info=True) }}
  {% endif %}
{% endmacro %}
