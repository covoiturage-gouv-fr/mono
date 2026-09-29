-- geo.perimeters_all : millésimes importés du datalake, une partition par millésime
-- (geo.perimeters_<année>). geo.perimeters reste la table actuelle jusqu'à la bascule
-- (src/db/geo/switch-to-millesimes.sql), jouée à la fin de just geo-import.
CREATE TABLE geo.perimeters_all (
  LIKE geo.perimeters INCLUDING DEFAULTS,
  valid_from date,
  valid_until date
) PARTITION BY LIST (year);

CREATE SEQUENCE geo.perimeters_all_id_seq OWNED BY geo.perimeters_all.id;
ALTER TABLE geo.perimeters_all ALTER COLUMN id SET DEFAULT nextval('geo.perimeters_all_id_seq');

CREATE INDEX geo_perimeters_all_id_idx ON geo.perimeters_all USING btree (id);
CREATE INDEX geo_perimeters_all_year_idx ON geo.perimeters_all USING btree (year);
CREATE INDEX geo_perimeters_all_arr_idx ON geo.perimeters_all USING btree (arr);
CREATE INDEX geo_perimeters_all_com_idx ON geo.perimeters_all USING btree (com);
CREATE INDEX geo_perimeters_all_epci_idx ON geo.perimeters_all USING btree (epci);
CREATE INDEX geo_perimeters_all_aom_idx ON geo.perimeters_all USING btree (aom);
CREATE INDEX geo_perimeters_all_dep_idx ON geo.perimeters_all USING btree (dep);
CREATE INDEX geo_perimeters_all_reg_idx ON geo.perimeters_all USING btree (reg);
CREATE INDEX geo_perimeters_all_country_idx ON geo.perimeters_all USING btree (country);
CREATE INDEX geo_perimeters_all_surface_idx ON geo.perimeters_all USING btree (surface);
CREATE INDEX geo_perimeters_all_centroid_idx ON geo.perimeters_all USING gist (centroid);
CREATE INDEX geo_perimeters_all_geom_idx ON geo.perimeters_all USING gist (geom);
CREATE INDEX geo_perimeters_all_geom_simple_idx ON geo.perimeters_all USING gist (geom_simple);

-- Crée geo.perimeters_<_year> depuis les lignes _year de _source (staging de l'import) et l'attache.
CREATE FUNCTION geo.attach_millesime(_source regclass, _year smallint, _replace boolean DEFAULT false)
  RETURNS bigint
  LANGUAGE plpgsql AS $$
DECLARE
  _partition text := format('perimeters_%s', _year);
  _cols text := 'year, centroid, geom, geom_simple, l_arr, arr, l_com, com, l_epci, epci, '
    'l_dep, dep, l_reg, reg, l_country, country, l_aom, aom, l_reseau, reseau, pop, surface, '
    'valid_from, valid_until';
  -- geo.perimeters (table avant la bascule, vue après) : le millésime en service.
  _latest smallint := (SELECT max(year) FROM geo.perimeters);
  _latest_rows bigint := (SELECT count(*) FROM geo.perimeters WHERE year = _latest);
  _rows bigint;
BEGIN
  IF to_regclass(format('geo.%I', _partition)) IS NOT NULL THEN
    IF NOT _replace THEN
      RAISE EXCEPTION 'geo.% existe déjà (_replace => true pour le remplacer)', _partition;
    END IF;
    EXECUTE format('DROP TABLE geo.%I', _partition);
  END IF;

  EXECUTE format('CREATE TABLE geo.%I (LIKE geo.perimeters_all INCLUDING DEFAULTS)', _partition);
  EXECUTE format('INSERT INTO geo.%I (%s) SELECT %s FROM %s WHERE year = $1', _partition, _cols, _cols, _source)
    USING _year;
  GET DIAGNOSTICS _rows = ROW_COUNT;

  IF _rows = 0 THEN
    RAISE EXCEPTION 'aucune ligne pour le millésime % dans %', _year, _source;
  END IF;

  -- Garde-fou : un millésime tronqué qui deviendrait le dernier casserait le géocodage.
  IF _year >= _latest AND _rows < _latest_rows * 0.9 THEN
    RAISE EXCEPTION 'millésime % : % lignes contre % pour le millésime %', _year, _rows, _latest_rows, _latest;
  END IF;

  EXECUTE format('ALTER TABLE geo.perimeters_all ATTACH PARTITION geo.%I FOR VALUES IN (%s)', _partition, _year);
  EXECUTE format('ANALYZE geo.%I', _partition);

  RETURN _rows;
END;
$$;
