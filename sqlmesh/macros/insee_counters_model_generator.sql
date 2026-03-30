{% macro insee_counters_model_generator(source_table, start_ts, end_ts) %}
SELECT
  j._id,
  j.start_datetime,
  j.start_geo_code,
  j.end_geo_code,
  COUNT(*) OVER (
    PARTITION BY date_trunc('month', j.start_datetime AT TIME ZONE 'Europe/Paris'),
                 j.start_geo_code
  ) AS start_insee_count,
  COUNT(*) OVER (
    PARTITION BY date_trunc('month', j.start_datetime AT TIME ZONE 'Europe/Paris'),
                 j.end_geo_code
  ) AS end_insee_count
FROM {{ source_table }} j
WHERE j.valid_acquisition_status = TRUE
  AND j.start_datetime >= {{ start_ts }}
  AND j.start_datetime < LEAST(
    {{ end_ts }},
    date_trunc('month', {{ end_ts }} AT TIME ZONE 'Europe/Paris')
      AT TIME ZONE 'Europe/Paris'
  )
{% endmacro %}
