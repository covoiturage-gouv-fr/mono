MODEL (
  name trusted_zone.journeys_2021,
  kind FULL,
  grain '_id',
  tags ['trusted', 'journeys', '2021'],
);

JINJA_QUERY_BEGIN;
{{ journeys_model_generator('2021-01-01','2021-12-31') }}
JINJA_END;

CREATE INDEX IF NOT EXISTS journeys_2021_id_index ON @this_model USING btree (_id);