-- Pas de fenêtre : materialized='table', snapshot complet à chaque run.
{{ config(severity='warn', tags=['trusted', 'policies']) }}

SELECT pp._id
FROM {{ source('dlk_import', 'policy_policies') }} AS pp
LEFT JOIN
  {{ source('dlk_import', 'territory_territory_group') }} AS ttg
  ON pp.territory_id = ttg._id
LEFT JOIN
  {{ source('dlk_import', 'company_companies') }} AS ccp
  ON ttg.company_id = ccp._id
INNER JOIN {{ ref('policies') }} AS t ON pp._id = t._id
WHERE
  pp.name IS DISTINCT FROM t.name
  OR pp.start_date IS DISTINCT FROM t.start_date
  OR pp.end_date IS DISTINCT FROM t.end_date
  OR pp.max_amount IS DISTINCT FROM t.max_amount
  OR pp.status::VARCHAR IS DISTINCT FROM t.status
  OR ttg.name IS DISTINCT FROM t.territory_name
  OR ccp.siret IS DISTINCT FROM t.territory_siret
