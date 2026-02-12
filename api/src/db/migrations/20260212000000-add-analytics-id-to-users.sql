-- Add analytics_id column (nullable first for backfill)
ALTER TABLE auth.users ADD COLUMN analytics_id VARCHAR(21) UNIQUE;

-- Backfill existing users
UPDATE auth.users
  SET analytics_id = substr(replace(replace(encode(gen_random_bytes(16), 'base64'), '+', '-'), '/', '_'), 1, 21)
  WHERE analytics_id IS NULL;

-- Set NOT NULL and default
ALTER TABLE auth.users
  ALTER COLUMN analytics_id SET NOT NULL,
  ALTER COLUMN analytics_id SET DEFAULT substr(replace(replace(encode(gen_random_bytes(16), 'base64'), '+', '-'), '/', '_'), 1, 21);
