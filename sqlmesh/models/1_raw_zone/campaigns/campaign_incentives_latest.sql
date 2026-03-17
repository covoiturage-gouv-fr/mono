MODEL (
  name raw_zone.campaign_incentives_latest,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column datetime,
  ),
  start '2025-01-01 00:00:00+0100',
  grain '_id',
  tags ['archive', 'campaign', 'incentives', 'latest'],
);

JINJA_QUERY_BEGIN;
{{ campaign_incentives("@start_ts", "@end_ts") }}
JINJA_END;

CREATE INDEX IF NOT EXISTS ci_latest_campaign_id_idx ON @this_model USING btree (campaign_id);
CREATE INDEX IF NOT EXISTS ci_latest_carpool_v2_id_idx ON @this_model USING btree (carpool_v2_id);
