{{ config(
  materialized='view',
  tags=['app_metabase', 'terms_violation']
) }}

-- Vue app_metabase (GEN-647) sur zone_aggregated.too_many_trips_flagged.
-- Owner-rights : datalake_mb_ro lit sans acces direct a zone_aggregated.
SELECT * FROM {{ ref('too_many_trips_flagged') }}
