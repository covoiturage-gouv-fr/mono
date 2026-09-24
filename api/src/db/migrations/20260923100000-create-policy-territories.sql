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

CREATE OR REPLACE FUNCTION geo.get_arr_by_selectors(types varchar[], codes varchar[], year smallint)
RETURNS TABLE(selector_type varchar, selector_value varchar, arr varchar)
LANGUAGE sql STABLE AS $$
  SELECT s.selector_type, s.selector_value, p.arr
  FROM unnest($1, $2) AS s(selector_type, selector_value)
  JOIN geo.perimeters p
    ON p.year = $3
    AND s.selector_value = CASE s.selector_type
      WHEN 'arr' THEN p.arr
      WHEN 'com' THEN p.com
      WHEN 'epci' THEN p.epci
      WHEN 'aom' THEN p.aom
      WHEN 'dep' THEN p.dep
      WHEN 'reg' THEN p.reg
    END
$$;

-- a com selector now also matches the arrondissements of Paris, Lyon and Marseille
CREATE OR REPLACE FUNCTION territory.get_com_by_territory_id(_id integer, year smallint)
RETURNS TABLE(com character varying)
LANGUAGE sql STABLE AS $$
  SELECT DISTINCT r.arr
  FROM (
    SELECT array_agg(selector_type) AS types, array_agg(selector_value) AS codes
    FROM territory.territory_group_selector
    WHERE territory_group_id = $1
  ) s
  CROSS JOIN LATERAL geo.get_arr_by_selectors(s.types, s.codes, $2) r
$$;
