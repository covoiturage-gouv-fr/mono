MODEL (
  name raw_zone.campaign_incentives_latest,
  kind INCREMENTAL_BY_UNIQUE_KEY (
    unique_key (_id)
  ),
  grain [_id],
  tags ['raw', 'campaign', 'incentives'],
--  audits (assert_campaign_incentives_complete),
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
  status::text,
  state::text
FROM archive_zone.archive_incentives_view
WHERE datetime >= date_trunc('year', current_date at time zone 'Europe/Paris')
ORDER BY datetime
;

