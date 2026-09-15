{{ config(severity='error', tags=['trusted', 'geo', 'custom_territories']) }}

-- L'id d'un territoire custom est un slug (jamais purement numérique) : c'est ce qui
-- garantit l'absence de collision avec les codes SIREN/INSEE des autres types dans
-- perimeters_agg (clé unique code, type, year).
SELECT id
FROM {{ ref('custom_territories_meta') }}
WHERE id !~ '^[a-z0-9-]{3,32}$' OR id ~ '^[0-9]+$'
