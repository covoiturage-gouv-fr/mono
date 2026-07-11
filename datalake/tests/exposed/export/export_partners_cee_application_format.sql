{{ config(severity='error', tags=['exposed', 'export']) }}

-- cee_application doit rester au format CSV historique '1'/'' (contrat partenaire),
-- pas le rendu booleen 't'/'f' de COPY. Fenetre bornee pour limiter le cout FDW.
SELECT cee_application
FROM {{ ref('export_partners') }}
WHERE start_datetime_tz >= CURRENT_DATE - INTERVAL '90 days'
  AND cee_application IS DISTINCT FROM '1'
  AND cee_application IS DISTINCT FROM ''
