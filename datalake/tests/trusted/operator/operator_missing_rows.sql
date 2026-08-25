-- Pas de fenêtre : materialized='table', snapshot complet à chaque run.
{{ config(severity='warn', tags=['trusted', 'operators']) }}

SELECT oo._id
FROM {{ source('dlk_import', 'operator_operators') }} AS oo
WHERE NOT EXISTS (
  SELECT 1 FROM {{ ref('operator') }} AS t
  WHERE t._id = oo._id
)
