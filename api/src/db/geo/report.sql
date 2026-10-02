-- Rapport sur geo_export (`import.sh check`), à lire avant `apply`. Lecture seule.
SELECT year, count(*) AS rows, min(valid_from) AS valid_from, max(valid_until) AS valid_until,
  count(*) FILTER (WHERE geom IS NULL OR geom_simple IS NULL OR centroid IS NULL) AS sans_geometrie,
  count(DISTINCT com) AS communes, count(DISTINCT epci) AS epci, count(DISTINCT aom) AS aom
FROM geo_export.perimeters
GROUP BY year ORDER BY year;

SELECT year, count(*) AS rows FROM geo_export.com_evolution GROUP BY year ORDER BY year;
