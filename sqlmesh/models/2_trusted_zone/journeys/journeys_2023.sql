MODEL (
  name trusted_zone.journeys_2023,
  kind FULL,
  grain '_id',
  tags ['trusted', 'journeys', '2023'],
);

JINJA_QUERY_BEGIN;
{{ journeys_model_generator('2023-01-01','2023-12-31') }}
JINJA_END;

CREATE INDEX IF NOT EXISTS journeys_2023_id_index ON @this_model USING btree (_id);