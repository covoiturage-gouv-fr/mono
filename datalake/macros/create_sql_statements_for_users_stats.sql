{% macro create_sql_statements_for_users_stats(statement, column_name) %}
    {{ statement }} as {{ column_name }},
    {{ statement }} filter (where role='driver') as {{ column_name }}_as_driver,
    {{ statement }} filter (where role='passenger') as {{ column_name }}_as_passenger
{% endmacro %}