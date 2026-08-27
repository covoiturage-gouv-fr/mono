{{ config(
  materialized='view',
  tags=['app_metabase', 'weekly_cee_per_week_long']
) }}

SELECT
  week_start,
  is_specific,
  operator,
  count
FROM {{ ref('cee_by_week') }}
WHERE
  journey_type = 'long'
  AND week_start < now() - interval '7 days'
ORDER BY week_start, is_specific, operator
