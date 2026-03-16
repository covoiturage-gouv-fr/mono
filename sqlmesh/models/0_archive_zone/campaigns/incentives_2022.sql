MODEL (
  name archive_zone.campaign_incentives_2022,
  kind INCREMENTAL_BY_TIME_RANGE (
    time_column datetime,
  ),
  start '2022-01-01 00:00:00+0100',
  end   '2022-12-31 23:59:59+0100',
  grain '_id',
  tags ['archive', 'campaign', 'incentives', '2022'],
);

JINJA_QUERY_BEGIN;
{{ campaign_incentives("@start_ts", "@end_ts") }}
JINJA_END;

CREATE INDEX IF NOT EXISTS ci2022_campaign_id_idx ON @this_model USING btree (campaign_id);
CREATE INDEX IF NOT EXISTS ci2022_carpool_v2_id_idx ON @this_model USING btree (carpool_v2_id);
