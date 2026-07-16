ALTER TABLE auth.users ADD COLUMN IF NOT EXISTS deletion_warned_at timestamptz;
