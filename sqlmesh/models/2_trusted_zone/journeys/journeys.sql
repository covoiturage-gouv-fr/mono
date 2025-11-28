MODEL (
  name trusted_zone.journeys,
  kind VIEW,
  grain '_id',
  tags ['trusted', 'journeys']
);

select * from trusted_zone.journeys_2020
UNION ALL
select * from trusted_zone.journeys_2021
UNION ALL
select * from trusted_zone.journeys_2022
UNION ALL
select * from trusted_zone.journeys_2023
UNION ALL
select * from trusted_zone.journeys_2024
UNION ALL
select * from trusted_zone.journeys_latest
ORDER BY _id desc
;