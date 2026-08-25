{{ config(
  materialized='view',
  tags=['app_metabase', 'weekly_cee_per_week_short']
) }}

SELECT
  week_start,
  is_specific,
  operator,
  count
FROM {{ ref('cee_by_week') }}
WHERE
  journey_type = 'short'
  AND week_start < now() - interval '7 days'
ORDER BY week_start, is_specific, operator
