{{ config(
  materialized='view',
  tags=['app_metabase', 'occupation']
) }}

-- Vue app_metabase (GEN-575) sur zone_exposed.occupation_year.
-- Owner-rights : datalake_mb_ro lit sans acces direct a zone_exposed.
SELECT * FROM {{ ref('occupation_year') }}
