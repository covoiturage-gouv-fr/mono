MODEL (
  name refined_zone.part_campaigns_month,
  kind INCREMENTAL_BY_UNIQUE_KEY (
    unique_key (year, month, campaign_id, operator_id)
  ),
  start '2020-01-01',
  cron '@monthly',
  tags ['refined', 'partenaires']
);

WITH last_period AS (
  SELECT COALESCE(MAX(a.year * 100 + a.month), 0)::INT AS value
  FROM refined_zone.part_campaigns_month as a
)
SELECT
  EXTRACT(YEAR FROM start_date)::INT  AS year,
  EXTRACT(MONTH FROM start_date)::INT AS month,
  campaign_id::INT AS campaign_id,
  operator_id::INT AS operator_id,
  SUM(journeys)::INT AS journeys,
  SUM(incented_journeys)::INT AS incented_journeys,
  SUM(incentive_amount)::INT  AS incentive_amount
FROM refined_zone.part_campaigns_day
WHERE
  (EXTRACT(YEAR FROM start_date)::INT * 100 + EXTRACT(MONTH FROM start_date)::INT)
  >= (SELECT value FROM last_period)
GROUP BY 1, 2, 3, 4;