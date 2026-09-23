CREATE TABLE policy.policy_territories (
  _id        serial PRIMARY KEY,
  policy_id  int NOT NULL REFERENCES policy.policies(_id),
  version    int NOT NULL,
  arr        varchar(5)[] NOT NULL,
  valid_from timestamptz NOT NULL,
  valid_to   timestamptz,
  created_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (policy_id, version),
  CHECK (cardinality(arr) > 0),
  CHECK (valid_to IS NULL OR valid_to > valid_from)
);
