-- H3 geospatial indexing extensions.
-- trusted.carpools indexes each trip's start/end position into H3 cells
-- (h3_lat_lng_to_cell over a PostGIS point) for the fraud / geo-pattern aggregates.
-- h3       = core H3 API (h3index type, cell functions).
-- h3_postgis = PostGIS bindings (accept geometry/point), pulls in postgis_raster.
-- CASCADE auto-installs the required postgis_raster dependency.
CREATE EXTENSION IF NOT EXISTS h3;
CREATE EXTENSION IF NOT EXISTS h3_postgis CASCADE;
