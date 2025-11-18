-- Add descriptive_sheet_url column to policy.policies table
ALTER TABLE policy.policies ADD COLUMN descriptive_sheet_url VARCHAR(512);
