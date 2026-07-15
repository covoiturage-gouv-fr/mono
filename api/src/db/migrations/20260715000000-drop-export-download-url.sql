-- Drop the now-unused download_url column: exports are downloaded through
-- GetDownloadLinkAction, which signs a fresh S3 URL from `filename` on demand.
-- The datalake export worker never populated this column, and the frontend no
-- longer reads it.
ALTER TABLE export.exports
DROP COLUMN download_url;
