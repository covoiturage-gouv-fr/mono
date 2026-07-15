-- Drop dead columns from export.exports.
--
-- download_url / download_url_expire_at: exports are downloaded through
-- GetDownloadLinkAction, which signs a fresh S3 URL from `filename` on demand.
-- Neither the datalake export worker nor the legacy command populated these, and
-- nothing reads them anymore.
--
-- stats: never written by any command/action and never exposed by ListAction —
-- a JSON field designed but never wired since the table was created.
ALTER TABLE export.exports
DROP COLUMN download_url,
DROP COLUMN download_url_expire_at,
DROP COLUMN stats;
