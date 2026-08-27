{{ config(
  materialized='table',
  indexes=[
    { 'columns': ['campaign_id'], 'unique': true }
  ],
  tags=['aggregated', 'policies', 'daily']
) }}

SELECT
  campaign_id,
  SUM(amount) FILTER (WHERE status = 'validated') AS validated,
  SUM(amount) FILTER (WHERE status = 'draft')     AS draft
FROM {{ ref('incentives') }}
GROUP BY campaign_id
