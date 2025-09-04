{% macro get_timezoned_timestamp(geo_code_col, datetime_col) %}
    CASE
      WHEN
        {{geo_code_col}}::text ~ '^97[1-2]'
        THEN {{datetime_col}} AT TIME ZONE 'America/Guadeloupe'
      WHEN
        {{geo_code_col}}::text ~ '^973'
        THEN {{datetime_col}} AT TIME ZONE 'America/Cayenne'
      WHEN
        {{geo_code_col}}::text ~ '^974'
        THEN {{datetime_col}} AT TIME ZONE 'Indian/Reunion'
      WHEN
        {{geo_code_col}}::text ~ '^976'
        THEN {{datetime_col}} AT TIME ZONE 'Indian/Mayotte'
      ELSE {{datetime_col}} AT TIME ZONE 'Europe/Paris'
    END
{% endmacro %}