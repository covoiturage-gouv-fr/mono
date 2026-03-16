MODEL (
  name archive_zone.campaign_incentives,
  kind VIEW (),
);

SELECT * FROM archive_zone.campaign_incentives_2019
UNION
SELECT * FROM archive_zone.campaign_incentives_2020
UNION
SELECT * FROM archive_zone.campaign_incentives_2021
UNION
SELECT * FROM archive_zone.campaign_incentives_2022
UNION
SELECT * FROM archive_zone.campaign_incentives_2023
UNION
SELECT * FROM archive_zone.campaign_incentives_2024
UNION
SELECT * FROM archive_zone.campaign_incentives_2025
;

