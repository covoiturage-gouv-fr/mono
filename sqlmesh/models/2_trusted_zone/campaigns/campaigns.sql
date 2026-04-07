MODEL (
  name trusted_zone.campaigns,
  kind VIEW (),
  tags ['trusted', 'campaign', 'incentives']
);

SELECT
  _id,
  carpool_v2_id,
  datetime,
  operator_id,
  operator_journey_id,
  campaign_id,
  campaign_name,
  territory_siret,
  territory_name,
  amount,
  result,
  status,
  state
FROM raw_zone.incentives;
