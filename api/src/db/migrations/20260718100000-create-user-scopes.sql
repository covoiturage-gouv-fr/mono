CREATE EXTENSION IF NOT EXISTS btree_gist;

CREATE TABLE auth.user_scopes (
  _id          serial PRIMARY KEY,
  user_id      int  NOT NULL REFERENCES auth.users(_id) ON DELETE CASCADE,
  operator_id  int  NULL REFERENCES operator.operators(_id),
  territory_id int  NULL REFERENCES territory.territory_group(_id),
  is_default   boolean NOT NULL DEFAULT false,
  created_at   timestamptz NOT NULL DEFAULT now(),
  scope_type   text GENERATED ALWAYS AS (CASE WHEN operator_id IS NOT NULL THEN 'o' ELSE 't' END) STORED,
  CHECK (num_nonnulls(operator_id, territory_id) = 1)
);
CREATE UNIQUE INDEX user_scopes_territory_key       ON auth.user_scopes(user_id, territory_id) WHERE territory_id IS NOT NULL;
CREATE UNIQUE INDEX user_scopes_single_operator_key ON auth.user_scopes(user_id)               WHERE operator_id  IS NOT NULL;
CREATE UNIQUE INDEX user_scopes_one_default_key     ON auth.user_scopes(user_id)               WHERE is_default;
ALTER TABLE auth.user_scopes
  ADD CONSTRAINT user_scopes_homogeneous_excl EXCLUDE USING gist (user_id WITH =, scope_type WITH <>);
CREATE INDEX user_scopes_operator_idx  ON auth.user_scopes(operator_id)  WHERE operator_id  IS NOT NULL;
CREATE INDEX user_scopes_territory_idx ON auth.user_scopes(territory_id) WHERE territory_id IS NOT NULL;

ALTER TABLE auth.users ADD COLUMN login_siren varchar(9);

-- backfill idempotent, opérateur prioritaire sur both-set
INSERT INTO auth.user_scopes (user_id, operator_id, is_default)
SELECT u._id, u.operator_id, true FROM auth.users u
WHERE u.operator_id IS NOT NULL
  AND NOT EXISTS (SELECT 1 FROM auth.user_scopes s WHERE s.user_id = u._id);

INSERT INTO auth.user_scopes (user_id, territory_id, is_default)
SELECT u._id, u.territory_id, true FROM auth.users u
WHERE u.territory_id IS NOT NULL AND u.operator_id IS NULL
  AND NOT EXISTS (SELECT 1 FROM auth.user_scopes s WHERE s.user_id = u._id);

UPDATE auth.users u SET login_siren = LEFT(COALESCE(o.siret, c.siret), 9)
FROM auth.users src
LEFT JOIN operator.operators o ON o._id = src.operator_id
LEFT JOIN territory.territory_group g ON g._id = src.territory_id
LEFT JOIN company.companies c ON c._id = g.company_id
WHERE u._id = src._id;
