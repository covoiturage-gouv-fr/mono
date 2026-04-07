MODEL (
  name trusted_zone.cee_grouped_journeys,
  kind FULL,
  start '2019-09-24 00:00:00+0200',
  end '2025-05-25 00:00:00+0200',
  grain (carpool_v2_id),
  tags ['trusted', 'cee'],
);

-- Root journeys: journeys with a CEE short application
-- where the driver is a primo-conducteur (first journey within 3 months of root)
WITH cee_roots AS (
  SELECT
    cee._id AS cee_application_id,
    cee.carpool_v2_id AS root_carpool_v2_id,
    j.driver_id,
    j.operator_id,
    j.start_datetime AS root_datetime
  FROM trusted_zone.cee_applications cee
  INNER JOIN trusted_zone.journeys j
    ON j._id = cee.carpool_v2_id
    AND j.valid_acquisition_status = true
  INNER JOIN trusted_zone.cee_primo_drivers p
    ON p.driver_hash = md5(j.driver_id)
    AND p.first_datetime BETWEEN j.start_datetime - INTERVAL '3 months'
                              AND j.start_datetime
  WHERE cee.journey_type = 'short'
),

-- The root journey itself
roots AS (
  SELECT DISTINCT
    root_carpool_v2_id AS carpool_v2_id,
    cee_application_id,
    root_carpool_v2_id
  FROM cee_roots
),

-- Related journeys: same driver, same operator, within 3 months of root
related AS (
  SELECT DISTINCT ON (j._id)
    j._id AS carpool_v2_id,
    r.cee_application_id,
    r.root_carpool_v2_id
  FROM cee_roots r
  INNER JOIN trusted_zone.journeys j
    ON j.driver_id = r.driver_id
    AND j.operator_id = r.operator_id
    AND j._id != r.root_carpool_v2_id
    AND j.start_datetime BETWEEN r.root_datetime - INTERVAL '3 months'
                              AND r.root_datetime
    AND j.valid_acquisition_status = true
  ORDER BY j._id, ABS(EXTRACT(EPOCH FROM j.start_datetime - r.root_datetime))
)

SELECT carpool_v2_id, cee_application_id, root_carpool_v2_id FROM roots
UNION ALL
SELECT carpool_v2_id, cee_application_id, root_carpool_v2_id FROM related;
