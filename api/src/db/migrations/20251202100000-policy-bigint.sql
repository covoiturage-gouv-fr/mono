ALTER TABLE policy.policies
ALTER COLUMN incentive_sum
TYPE bigint
USING incentive_sum::bigint;

ALTER TABLE policy.policy_metas
ALTER COLUMN value
TYPE bigint
USING value::bigint
