ALTER TABLE territory.territory_group ALTER COLUMN company_id DROP NOT NULL;

CREATE TABLE territory.territory_perimeters (
  _id          serial PRIMARY KEY,
  territory_id int NOT NULL REFERENCES territory.territory_group(_id),
  version      int NOT NULL,
  arr          varchar(5)[] NOT NULL,
  valid_from   timestamptz NOT NULL,
  valid_to     timestamptz,
  created_at   timestamptz NOT NULL DEFAULT now(),
  UNIQUE (territory_id, version),
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

-- every loaded millesime, so that trips geocoded with the previous one keep matching after a merge
CREATE OR REPLACE FUNCTION territory.get_arr_by_selectors(_id integer)
RETURNS TABLE(arr varchar)
LANGUAGE sql STABLE AS $$
  SELECT DISTINCT r.arr
  FROM (
    SELECT array_agg(selector_type) AS types, array_agg(selector_value) AS codes
    FROM territory.territory_group_selector
    WHERE territory_group_id = $1
  ) s
  CROSS JOIN (SELECT DISTINCT year FROM geo.perimeters) y
  CROSS JOIN LATERAL geo.get_arr_by_selectors(s.types, s.codes, y.year) r
$$;

CREATE OR REPLACE FUNCTION territory.get_arr(_id integer, _at timestamptz)
RETURNS TABLE(arr varchar)
LANGUAGE sql STABLE AS $$
  WITH version AS (
    SELECT tp.arr
    FROM territory.territory_perimeters tp
    WHERE tp.territory_id = $1
      AND tp.valid_from <= $2
      AND (tp.valid_to IS NULL OR $2 < tp.valid_to)
    ORDER BY tp.version DESC
    LIMIT 1
  )
  SELECT unnest(v.arr) FROM version v
  UNION ALL
  SELECT s.arr FROM territory.get_arr_by_selectors($1) s
  WHERE NOT EXISTS (SELECT 1 FROM version)
$$;

-- union over [_from, _to): may overflow when a version changes mid-period, callers filter by trip date
CREATE OR REPLACE FUNCTION territory.get_arr_range(_id integer, _from timestamptz, _to timestamptz)
RETURNS TABLE(arr varchar)
LANGUAGE sql STABLE AS $$
  WITH versions AS (
    SELECT tp.arr, tstzrange(tp.valid_from, tp.valid_to) AS validity
    FROM territory.territory_perimeters tp
    WHERE tp.territory_id = $1
      AND tstzrange(tp.valid_from, tp.valid_to) && tstzrange($2, $3)
  )
  SELECT unnest(v.arr) FROM versions v
  UNION
  SELECT s.arr FROM territory.get_arr_by_selectors($1) s
  WHERE NOT COALESCE((SELECT range_agg(validity) FROM versions) @> tstzrange($2, $3), false)
$$;
