-- Drop the unused progress column from exports table
ALTER TABLE export.exports
DROP COLUMN progress;
