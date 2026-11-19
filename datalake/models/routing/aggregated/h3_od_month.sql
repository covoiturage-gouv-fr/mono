{{ config(
  materialized='table',
  tags=['aggregated'],
  indexes=[
    { 'columns': ['year', 'month', 'h3_1', 'h3_2', 'arr_1', 'arr_2'], 'unique': true },
    { 'columns': ['h3_1', 'h3_2'] },
  ]
) }}

-- Flux orientés entre cellules H3 z8 (départ → arrivée) des trajets de la
-- fenêtre de routage, même périmètre que /flux : entre deux communes, 80 km
-- au plus. Les arrondissements servent à la carte territoriale (from/to).
{% set start = "'" ~ var('routing_from') ~ " 00:00:00'" %}
WITH carpools AS (
  {{ filtered_carpools('arr', default_start=start, with_new_users=false) }}
),

od AS (
  SELECT
    start_h3index_z8                          AS h3_1,
    end_h3index_z8                            AS h3_2,
    start_code                                AS arr_1,
    end_code                                  AS arr_2,
    extract(YEAR FROM carpool_datetime)::int  AS year,  -- noqa: RF04
    extract(MONTH FROM carpool_datetime)::int AS month,  -- noqa: RF04
    count(*)                                  AS journeys
  FROM carpools
  WHERE
    carpool_datetime < '{{ var("routing_to") }}'
    AND NOT is_intra
    AND distance <= 80000
    AND start_h3index_z8 <> end_h3index_z8
  GROUP BY 1, 2, 3, 4, 5, 6
)

-- Centre de cellule arrondi comme la clé du cache zone_raw.osrm_route
SELECT
  od.*,
  round(st_x(h3_cell_to_geometry(od.h3_1))::numeric, 6) AS lng_1,
  round(st_y(h3_cell_to_geometry(od.h3_1))::numeric, 6) AS lat_1,
  round(st_x(h3_cell_to_geometry(od.h3_2))::numeric, 6) AS lng_2,
  round(st_y(h3_cell_to_geometry(od.h3_2))::numeric, 6) AS lat_2
FROM od
