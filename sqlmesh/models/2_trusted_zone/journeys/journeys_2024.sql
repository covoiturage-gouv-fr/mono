MODEL (
  name trusted_zone.journeys_2024,
  kind FULL,
  grain '_id',
  tags ['trusted', 'journeys', '2024'],
);
JINJA_QUERY_BEGIN;
{{ journeys_model_generator('2024-01-01','2024-12-31') }}
JINJA_END;

CREATE INDEX IF NOT EXISTS journeys_2024_id_index ON @this_model USING btree (_id);