{% macro carpools_perim_join() %}
LEFT JOIN {{ ref('perimeters') }} ps ON ps.arr = c.start_geo_code AND c.start_datetime_tz >= ps.valid_from AND c.start_datetime_tz < ps.valid_until
LEFT JOIN {{ ref('perimeters') }} pe ON pe.arr = c.end_geo_code AND c.start_datetime_tz >= pe.valid_from AND c.start_datetime_tz < pe.valid_until
{%- endmacro %}