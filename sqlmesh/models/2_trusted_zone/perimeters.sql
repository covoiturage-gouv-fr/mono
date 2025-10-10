MODEL (
  name trusted_zone.perimeters,
  kind FULL,
  grains ['id','year','arr']
);

WITH old_perimeters AS (
  SELECT 
    a.year, 
    a.arr, 
    a.l_arr, 
    a.com, 
    a.l_com,
    a.epci, 
    a.l_epci, 
    a.aom, 
    a.l_aom, 
    a.dep, 
    a.l_dep,
    a.reg,
    a.l_reg,
    a.country,
    a.l_country,
    a.pop, 
    a.surface,
    b.geometry AS geom,
    a.geometry AS geom_simple,
    c.geometry AS centroid
  FROM raw_zone.old_perimeters_simple a
  LEFT JOIN raw_zone.old_perimeters_full b ON a.year = b.year AND a.arr = b.arr
  LEFT JOIN raw_zone.old_perimeters_centroid c ON a.year = c.year AND a.arr = c.arr
),
new_com AS (
  SELECT 
    2025 AS year,
    a.com AS arr, 
    a.l_com AS l_arr, 
    a.com, 
    a.l_com,
    a.epci,
    b.l_epci,
    e.aom,
    e.l_aom,
    a.dep,
    c.l_dep,
    a.reg,
    d.l_reg,
    'XXXXX' AS country,
    'France' AS l_country,
    a.population AS pop,
    ST_Area(f.geometry::geography) / 1000000 AS surface,
    f.geometry AS geom,
    a.geometry AS geom_simple,
    g.geometry AS centroid
  FROM raw_zone.ign_aecarto_com_2025 a
  LEFT JOIN raw_zone.ign_aecarto_epci_2025 b ON a.epci = b.epci
  LEFT JOIN raw_zone.ign_aecarto_dep_2025 c ON a.dep = c.dep
  LEFT JOIN raw_zone.ign_aecarto_reg_2025 d ON a.reg = d.reg
  LEFT JOIN raw_zone.cerema_aom_2025 e ON a.com = e.com
  LEFT JOIN raw_zone.ign_ae_com_2025 f ON a.com = f.com
  LEFT JOIN raw_zone.ign_aecentroid_com_2025 g ON a.com = g.com 
),
new_arr AS (
  SELECT 
    b.year,
    a.arr, 
    a.l_arr, 
    a.com, 
    b.l_com,
    b.epci,
    b.l_epci,
    b.aom,
    b.l_aom,
    b.dep,
    b.l_dep,
    b.reg,
    b.l_reg,
    b.country,
    b.l_country,
    a.population AS pop,
    ST_Area(c.geometry::geography) / 1000000 AS surface,
    c.geometry AS geom,
    a.geometry AS geom_simple,
    d.geometry AS centroid
  FROM raw_zone.ign_aecarto_arr_2025 a
  LEFT JOIN new_com b ON a.com = b.arr
  LEFT JOIN raw_zone.ign_ae_arr_2025 c ON a.arr = c.arr
  LEFT JOIN raw_zone.ign_aecentroid_arr_2025 d ON a.arr = d.arr
),
new_country AS (
  SELECT 
    2025 AS year,
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
    surface,
    geom,
    geom_simple,
    centroid
  FROM old_perimeters
  WHERE year = 2024 AND country <> 'XXXXX'
),
-- 👇 Agrégation globale
all_perimeters AS (
  SELECT * FROM old_perimeters
  UNION ALL
  SELECT * FROM new_com
  UNION ALL
  SELECT * FROM new_arr
  UNION ALL
  SELECT * FROM new_country
)
-- 👇 Ajout de l’ID unique sur tout l’ensemble
SELECT
  ROW_NUMBER() OVER (ORDER BY year, arr) AS id,
  *
FROM all_perimeters
ORDER BY year, arr;

CREATE INDEX IF NOT EXISTS perimeters_id_index ON @this_model USING btree (id);