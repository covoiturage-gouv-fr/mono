MODEL (
  name trusted_zone.journeys_2022,
  kind FULL,
  grain '_id',
  tags ['trusted', 'journeys', '2022'],
);

JINJA_QUERY_BEGIN;
{{ journeys_model_generator('2022-01-01','2022-12-31') }}
JINJA_END;

CREATE INDEX IF NOT EXISTS journeys_2022_id_index ON @this_model USING btree (_id);