MODEL (
  name refined_zone.obs_directions_day,
  kind VIEW,
  grain ['code', 'type', 'journey_date', 'direction'],
  tags ['refined', 'observatoire', 'directions_day']
);

select * from refined_zone.obs_directions_day_2020
UNION ALL
select * from refined_zone.obs_directions_day_2021
UNION ALL
select * from refined_zone.obs_directions_day_2022
UNION ALL
select * from refined_zone.obs_directions_day_2023
UNION ALL
select * from refined_zone.obs_directions_day_2024
UNION ALL
select * from refined_zone.obs_directions_day_2025
UNION ALL
select * from refined_zone.obs_directions_day_latest
