-- Pas de fenêtre : materialized='table', snapshot complet à chaque run.
{{ config(severity='warn', tags=['trusted', 'policies']) }}

SELECT 1 AS failure
WHERE (
  SELECT COUNT(*) FROM {{ source('dlk_import', 'policy_policies') }}
) != (
  SELECT COUNT(*) FROM {{ ref('policies') }}
)
