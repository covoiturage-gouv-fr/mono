MODEL (
  name trusted_zone.campaign_incentives,
  kind VIEW (),
  tags ['trusted', 'campaign', 'incentives'],
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
  status
FROM raw_zone.campaign_incentives_2019
UNION
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
  status
FROM raw_zone.campaign_incentives_2020
UNION
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
  status
FROM raw_zone.campaign_incentives_2021
UNION
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
  status
FROM raw_zone.campaign_incentives_2022
UNION
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
  status
FROM raw_zone.campaign_incentives_2023
UNION
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
  status
FROM raw_zone.campaign_incentives_2024
UNION
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
  status
FROM raw_zone.campaign_incentives_2025
UNION
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
  status
FROM raw_zone.campaign_incentives_latest
;

