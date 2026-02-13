ALTER TABLE auth.users ADD COLUMN IF NOT EXISTS analytics_id UUID DEFAULT gen_random_uuid();
ALTER TABLE auth.users ALTER COLUMN analytics_id SET NOT NULL;
