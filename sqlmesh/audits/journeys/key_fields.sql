-- Audit: Key field consistency for journeys
-- For rows that exist in both source and target, verifies critical columns match.
-- Checked fields: operator_id, start_datetime, distance, driver_revenue, passenger_contribution

-- Direction: PG → Raw
-- Attach to: raw_zone.journeys_* models
AUDIT (
  name assert_journeys_key_fields_pg_to_raw,
  blocking false
);

SELECT c._id
FROM carpool_v2.carpools c
INNER JOIN @this_model t ON t._id = c._id
WHERE c.start_datetime >= @start_ts::timestamp
  AND c.start_datetime <  @end_ts::timestamp
  AND (
    c.operator_id             IS DISTINCT FROM t.operator_id
    OR c.start_datetime       IS DISTINCT FROM t.start_datetime
    OR c.distance             IS DISTINCT FROM t.distance
    OR c.driver_revenue       IS DISTINCT FROM t.driver_revenue
    OR c.passenger_contribution IS DISTINCT FROM t.passenger_contribution
  );
