-- Pas de fenêtre : materialized='table', snapshot complet à chaque run.
{{ config(severity='warn', tags=['trusted', 'policies']) }}

SELECT pp._id
FROM {{ source('dlk_import', 'policy_policies') }} AS pp
WHERE NOT EXISTS (
  SELECT 1 FROM {{ ref('policies') }} AS t
  WHERE t._id = pp._id
)
