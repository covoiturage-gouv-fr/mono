-- Garde-fou anti double-comptage sur le GROUP BY / join operator : pas de
-- fenêtre, cee_by_week est une table complète sur une source cee figée.
{{ config(severity='warn', tags=['aggregated', 'cee']) }}

SELECT 1 AS failure
WHERE (
  SELECT COALESCE(SUM(count), 0) FROM {{ ref('cee_by_week') }}
) != (
  SELECT COUNT(*) FROM {{ ref('cee') }}
)
