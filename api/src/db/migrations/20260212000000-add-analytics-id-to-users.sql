-- Add analytics_id column (nullable first for backfill)
ALTER TABLE auth.users ADD COLUMN analytics_id UUID UNIQUE;

-- Backfill existing users
UPDATE auth.users
  SET analytics_id = gen_random_uuid()
  WHERE analytics_id IS NULL;

-- Set NOT NULL and default
ALTER TABLE auth.users
  ALTER COLUMN analytics_id SET NOT NULL,
  ALTER COLUMN analytics_id SET DEFAULT gen_random_uuid();
