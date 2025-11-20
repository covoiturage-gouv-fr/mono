MODEL (
  name trusted_zone.journeys,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column start_datetime,
    lookback 1
  ),
  start '2020-01-01',
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
; 

CREATE INDEX IF NOT EXISTS journeys_id_index ON @this_model USING btree (_id);