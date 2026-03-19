MODEL (
  name trusted_zone.cee_applications,
  kind VIEW,
  grain [uuid],
  tags ['trusted', 'cee'],
);

SELECT * FROM raw_zone.cee_applications;
