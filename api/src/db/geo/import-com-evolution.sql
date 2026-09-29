-- Remplace les années couvertes par l'export datalake (depuis 2020) ; la prod garde les
-- mouvements antérieurs (depuis 2019).
DELETE FROM geo.com_evolution
WHERE year >= (SELECT min(year) FROM geo_export.com_evolution);

INSERT INTO geo.com_evolution (year, mod, old_com, new_com, l_mod)
SELECT year, mod, old_com, new_com, l_mod FROM geo_export.com_evolution;
