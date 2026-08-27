{{ config(
  materialized='table',
  indexes=[
    { 'columns': ['_id'], 'unique': true }
  ],
  tags=['trusted', 'policies', 'daily']
) }}

SELECT
  pp._id,
  pp.name,
  pp.start_date,
  pp.end_date,
  pp.max_amount,
  pp.status::VARCHAR AS status,
  ttg.name           AS territory_name,
  ccp.siret          AS territory_siret
FROM {{ source('dlk_import', 'policy_policies') }} AS pp
LEFT JOIN
  {{ source('dlk_import', 'territory_territory_group') }} AS ttg
  ON pp.territory_id = ttg._id
LEFT JOIN
  {{ source('dlk_import', 'company_companies') }} AS ccp
  ON ttg.company_id = ccp._id
