-- Alter download_url column to support longer URLs
ALTER TABLE export.exports
ALTER COLUMN download_url TYPE text;

-- Add filename and file_size columns to exports table
ALTER TABLE export.exports
ADD COLUMN filename varchar(255),
ADD COLUMN file_size bigint;