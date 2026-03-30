MODEL (
  name trusted_zone.journeys,
  kind VIEW,
  grain ['code', 'type', 'journey_date', 'direction'],
  tags ['refined', 'observatoire', 'directions_day']
);

select * from refined_zone.obs_directions_day_latest
