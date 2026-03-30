MODEL (
  name trusted_zone.insee_counters,
  kind VIEW,
  grain '_id',
  tags ['trusted', 'insee_counters'],
);

SELECT * FROM trusted_zone.insee_counters_2020
UNION ALL
SELECT * FROM trusted_zone.insee_counters_2021
UNION ALL
SELECT * FROM trusted_zone.insee_counters_2022
UNION ALL
SELECT * FROM trusted_zone.insee_counters_2023
UNION ALL
SELECT * FROM trusted_zone.insee_counters_2024
UNION ALL
SELECT * FROM trusted_zone.insee_counters_2025
UNION ALL
SELECT * FROM trusted_zone.insee_counters_latest;
