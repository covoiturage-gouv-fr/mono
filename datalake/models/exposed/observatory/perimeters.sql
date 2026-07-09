{{ config(
    materialized='view',
    tags=['exposed', 'observatory', 'perimeters']
) }}

-- Périmètres géographiques exposés à l'API (remplace `geo.perimeters` côté API Deno).
-- L'API ne lit que la zone exposée : cette vue projette la table trusted pour la
-- résolution territoire -> communes (heatmap, flux, occupation, infra) et les libellés.

SELECT
    year,
    arr,
    l_arr,
    com,
    l_com,
    epci,
    l_epci,
    aom,
    l_aom,
    dep,
    l_dep,
    reg,
    l_reg,
    country,
    l_country,
    pop,
    surface
FROM {{ ref('perimeters') }}
