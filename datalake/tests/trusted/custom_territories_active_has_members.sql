{{ config(severity='error', tags=['trusted', 'geo', 'custom_territories']) }}

-- Tout composite actif doit avoir au moins une commune résolue, sinon il n'apparaît pas
-- dans perimeters_agg alors qu'il est censé être exposé.
SELECT m.id
FROM {{ ref('custom_territories_meta') }} AS m
LEFT JOIN {{ ref('custom_territories') }} AS ct ON m.id = ct.id
WHERE m.active
GROUP BY m.id
HAVING COUNT(ct.arr) = 0
