-- Add analytics_id column 
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
ALTER TABLE auth.users ADD COLUMN analytics_id UUID UNIQUE NOT NULL DEFAULT uuid_generate_v4();