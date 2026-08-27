-- Pas de fenêtre : materialized='table', snapshot complet à chaque run.
{{ config(severity='warn', tags=['trusted', 'operators']) }}

SELECT oo._id
FROM {{ source('dlk_import', 'operator_operators') }} AS oo
INNER JOIN {{ ref('operator') }} AS t ON oo._id = t._id
WHERE
  oo.name IS DISTINCT FROM t.name
  OR oo.siret IS DISTINCT FROM t.siret
