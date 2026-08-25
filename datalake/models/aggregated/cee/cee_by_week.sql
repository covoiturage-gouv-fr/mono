{{ config(
  materialized='table',
  indexes=[
    { 'columns': ['week_start', 'journey_type', 'is_specific', 'operator_id'], 'unique': true }
  ],
  tags=['aggregated', 'cee']
) }}

SELECT
  cee.journey_type,
  cee.is_specific,
  cee.operator_id,
  op.name
    AS operator,
  date_trunc('week', cee.datetime AT TIME ZONE 'Europe/Paris')
  ::date   AS week_start,
  count(*) AS count
FROM {{ ref('cee') }} AS cee
INNER JOIN {{ ref('operator') }} AS op ON cee.operator_id = op._id
GROUP BY 1, 2, 3, 4, 5
