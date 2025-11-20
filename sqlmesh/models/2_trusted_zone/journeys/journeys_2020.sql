MODEL (
  name trusted_zone.journeys_2020,
  kind FULL,
  grain '_id',
  tags ['trusted', 'journeys', '2020'],
);

JINJA_QUERY_BEGIN;
{{ journeys_model_generator('2020-01-01','2020-12-31') }}
JINJA_END;

CREATE INDEX IF NOT EXISTS journeys_2020_id_index ON @this_model USING btree (_id);