import marimo

__generated_with = "0.16.5"
app = marimo.App(width="medium")


@app.cell
def _():
    import marimo as mo
    return (mo,)


@app.cell
def _():
    import json
    import math
    import os
    from datetime import datetime, timedelta
    from itertools import product
    from pathlib import Path
    from zoneinfo import ZoneInfo
    from dotenv import load_dotenv
    import requests
    import time

    import branca.colormap as bcm
    import duckdb
    import folium
    import geopandas as gpd
    import matplotlib.cm as cm
    import matplotlib.colors as mcolors
    import matplotlib.pyplot as plt
    import plotly.express as px
    import plotly.graph_objects as go
    import polars as pl
    import polars_h3 as plh3
    import polars_st as st
    import shapely
    from dotenv import load_dotenv
    from folium import plugins
    from sqlalchemy import create_engine
    return (
        Path,
        ZoneInfo,
        bcm,
        cm,
        create_engine,
        datetime,
        duckdb,
        folium,
        gpd,
        load_dotenv,
        mcolors,
        os,
        pl,
        plh3,
        shapely,
    )


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""# Config""")
    return


@app.cell
def _(Path, ZoneInfo, datetime, load_dotenv, os):
    USE_CACHED_ARTIFACTS = False
    USE_CACHED_JOURNEYS_WITH_NEAREST_STATION = False


    START_DATE = datetime(2024, 9, 1, tzinfo=ZoneInfo("GMT"))
    ENTITY_CONFIGS = {
        "driver": {
            "identity_col": "driver_identity_key",
            "first_trip_col": "first_trip_datetime",
            "label_plural": "conducteurs",
            "label_singular": "conducteur",
        },
        "passenger": {
            "identity_col": "passenger_identity_key",
            "first_trip_col": "passenger_first_trip_datetime", 
            "label_plural": "passagers",
            "label_singular": "passager",
        }
    }

    load_dotenv()
    DB_URL = os.environ.get("DB_URL")
    RENNES_EXCLUSION_GEOJSON = os.environ.get("RENNES_EXCLUSION_GEOJSON")

    AOM_SIRET = "24350013900189"

    OUTPUT_PATH = Path("outputs_rennes")
    return (
        AOM_SIRET,
        DB_URL,
        OUTPUT_PATH,
        RENNES_EXCLUSION_GEOJSON,
        USE_CACHED_ARTIFACTS,
    )


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""## Labels""")
    return


@app.cell
def _():
    labels_map = {
        "month": "Mois",
        "num_journeys": "Nombre de journeys",
        "share_journeys": "% des journeys",
        "num_journeys_incentived": "Nombre de journey avec incitation",
        "num_journeys_with_incentive": "Nombre de journey avec incitation",
        "num_journeys_intra_territory_incentived_trips": "Nombre de journeys incitées intra",
        "num_journeys_inter_territory_incentived_trips": "Nombre de journeys incitées inter",
        "operator": "Opérateur",
        "incentive_amount_avg": "Incitation moyenne",
        "driver_revenue_avg": "Revenu moyen conducteur",
        "passenger_contribution_avg": "Contribution moyenne passager",
        "incentive_amount_intra_avg": "Incitation moyenne intra",
        "driver_revenue_intra_avg": "Revenu moyen conducteur intra",
        "passenger_contribution_intra_avg": "Contribution moyenne passager intra",
        "incentive_amount_inter_avg": "Incitation moyenne inter",
        "driver_revenue_inter_avg": "Revenu moyen conducteur inter",
        "passenger_contribution_inter_avg": "Contribution moyenne passager inter",
        "incentive_amount_per_km_avg": "Montant moyen d'incitation par km",
        "passenger_contribution_per_km_avg": "Contribution moyenne passager par km",
        "driver_revenue_per_km_avg": "Revenu moyen conducteur par km",
        "week": "Semaine",
        "month": "Mois",
        "year_month": "Mois",
        "distance_avg": "Distance moyenne",
        "distance_km": "Distance [km]",
        "distance_incentived_trips_avg": "Distance moyenne [km] - journeys avec incentives",
        "campaign_type": "Campagne",
        "distance": "Distance",
        "num_journeys_with_aom_incentive": "Nombre de journeys incitées par l'AOM",
        "num_journeys_with_operator_incentive": "Nombre de journeys incitées par un opérateur",
        "num_journeys_intra_territory": "Nombre de journeys intra-territoire",
        "num_journeys_inter_territory": "Nombre de journeys inter-territoires",
        "share_journeys_intra_territory": "% de journeys intra-territoire",
        "share_journeys_inter_territory": "% de journeys inter-territoires",
        "share_drivers": "% des conducteurs",
        "num_trips": "Nombre de trips",
        "is_intra_driver": "Conducteur intra",
        "driver_campaign_type": "Type de campagne du conducteur",
        "passenger_campaign_type": "Type de campagne du passager",
        "drivers_share": "% des conducteurs",
        "week_number": "Semaine n°",
        "passengers_share": "% des passagers",
        "num_passenger": "Nombre de passager",
        "is_near_station_fmt": "Catégorie de distance à une gare",
        "has_direct_train_line": "Possède une ligne TC directe",
        "name": "Nom",
        "amount_aom_avg" : "Incitation AOM moyenne",
        "True":"Vrai",
        "False":"Faux"
    }
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""## Duckdb""")
    return


@app.cell
def _(duckdb):
    conn = duckdb.connect(
        "db_rennes.duckdb",
        config={"memory_limit": "16GiB", "threads": 4, "preserve_insertion_order": False},
    )
    return (conn,)


@app.cell
def _(conn, mo):
    _df = mo.sql(
        f"""
        INSTALL spatial;
        LOAD spatial;
        """,
        engine=conn
    )
    return


@app.cell
def _(mo):
    mo.md(r"""## Queries""")
    return


@app.cell
def _(DB_URL, create_engine):
    SQL_ENGINE = create_engine(DB_URL)
    return (SQL_ENGINE,)


@app.cell
def _(mo):
    mo.md(r"""### Journeys""")
    return


@app.cell
def _(SQL_ENGINE, USE_CACHED_ARTIFACTS, pl):
    SQL = """with rennes_perimeter as 
    (
    select
    	p.arr,
    	(p.com) as com,
    	(p.geom_simple) as geom_simple,
        p.aom
    from
    	geo.perimeters p
    where
    	p.reg = '53'
        and p.dep='35'
        and p.aom='243500139'
    	and year = 2024
    ),
    geo_filtered as (
    select
    	g.carpool_id,
    	g.start_geo_code,
    	g.end_geo_code,
    	(rp_start.com IS NOT NULL AND rp_end.com IS NOT NULL) as is_fully_inside_campaign_area
    from
    	carpool_v2.geo g
    	left join rennes_perimeter rp_start 
    		on (g.start_geo_code) = rp_start.com
    	left join rennes_perimeter rp_end 
    		on (g.end_geo_code) = rp_end.com and g.updated_at >= '2024-01-01'
    where
    	(rp_start.com IS NOT NULL OR rp_end.com IS NOT NULL)
    ),
    first_trip as (
    select
        driver_identity_key,
        min(c.start_datetime) as first_trip_datetime
    from carpool_v2.carpools c
    group by 1
    ),
    first_trip_passengers as (
    select
        passenger_identity_key,
        min(c.start_datetime) as first_trip_datetime
    from carpool_v2.carpools c
    group by 1
    ),
    incentives as (
    select
    	oi.carpool_id,
    	sum(oi.amount) as incentive_amount,
        sum(oi.amount) filter (where siret='24350013900189') as amount_aom,
    	array_agg(distinct oi.siret) as incentive_sirets
    from
    	carpool_v2.operator_incentives oi
    inner join geo_filtered g on
    	oi.carpool_id = g.carpool_id
    where amount>0
    group by
    	1
    ),
    journeys as 
    (
    select
    	c."_id",
    	c.operator_id,
    	c.operator_journey_id,
    	c.operator_trip_id,
        c.driver_identity_key,
        ft.first_trip_datetime,
        c.passenger_identity_key,
        ftp.first_trip_datetime as passenger_first_trip_datetime,
    	c.start_datetime,
    	c.end_datetime,
    	c.distance,
    	c.driver_revenue,
    	c.passenger_contribution,
    	i.incentive_amount,
        i.amount_aom,
    	i.incentive_sirets,
    	c.start_position,
    	c.end_position,
        c.passenger_seats,
        c.passenger_travelpass_name,
        c.passenger_travelpass_user_id,
        is_fully_inside_campaign_area,
    	ST_MAKELINE(c.start_position::geometry,c.end_position::geometry) as journey_line,
        t.labels	
    from
    	carpool_v2.carpools c
    inner join geo_filtered g on
    	c."_id" = g.carpool_id
    left join incentives i on
    	c."_id" = i.carpool_id
    left join first_trip ft on ft.driver_identity_key=c.driver_identity_key
    left join first_trip_passengers ftp on ftp.passenger_identity_key=c.passenger_identity_key
    left join carpool_v2.status s on s."carpool_id"=c."_id" 
    left join carpool_v2.terms_violation_error_labels t on t."carpool_id"=c."_id" 
    where
    	(c.start_datetime between '2024-01-01' and '2025-07-20')
    	and s.acquisition_status='processed'
        and s.fraud_status='passed'
        and s.anomaly_status='passed'
        )
    SELECT
        j.*,
        CASE WHEN p.l_arr = p.country THEN p.l_country ELSE p.l_arr END as start_com,
        CASE WHEN p2.l_arr = p2.country THEN p2.l_country ELSE p2.l_arr END as end_com
    from journeys j
    left join carpool_v2.geo g on j."_id"=g."carpool_id"
    left join geo.perimeters p on g."start_geo_code"=p.arr  and p.year=2024
    left join geo.perimeters p2 on g."end_geo_code"=p2.arr and p2.year=2024"""

    if USE_CACHED_ARTIFACTS:
        df_journeys_raw = pl.read_parquet("df_journeys_raw_rennes.parquet")
    else:
        df_journeys_raw = pl.read_database(
            query=SQL,
            connection=SQL_ENGINE,
            infer_schema_length=10000,
            schema_overrides={
                "passenger_travelpass_name": pl.String,
                "passenger_travelpass_user_id": pl.String,
            },
        )
        df_journeys_raw.write_parquet("df_journeys_raw_rennes.parquet", compression_level=6)
    return (df_journeys_raw,)


@app.cell
def _(df_journeys_raw):
    df_journeys_raw.schema
    return


@app.cell
def _(df_journeys_raw):
    df_journeys_raw.estimated_size() / 1e7
    return


@app.cell
def _(df_journeys_raw):
    df_journeys_raw.head()
    return


@app.cell
def _(df_journeys_raw):
    df_journeys_raw.describe()
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""### Opérateurs""")
    return


@app.cell
def _(SQL_ENGINE, mo):
    df_operators_missing = mo.sql(
        f"""
        SELECT
            "_id",
            "name",
            "siret"
        from operator.operators
        where deleted_at is null
        and name!='BlaBlaCar'
        """,
        engine=SQL_ENGINE
    )
    return (df_operators_missing,)


@app.cell
def _(df_operators_missing):
    df_operators_missing
    return


@app.cell
def _(df_operators_missing, pl):
    df_karos=pl.DataFrame({"_id":[999], "name":["Karos"], "siret":["80279897500024"]})
    df_operators = pl.concat([df_operators_missing, df_karos])
    df_operators
    return (df_operators,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""# Zone d'exclusion Rennes Métropole""")
    return


@app.cell
def _(RENNES_EXCLUSION_GEOJSON, conn, mo):
    _df = mo.sql(
        f"""
        CREATE TABLE
          IF NOT EXISTS exclusion_rennes_metropole AS
        SELECT

          (ST_Transform (geom, 'EPSG:3857', 'EPSG:4326')) AS geom
        FROM
            ST_Read('{RENNES_EXCLUSION_GEOJSON}')
        """,
        engine=conn
    )
    return (exclusion_rennes_metropole,)


@app.cell
def _(conn, mo):
    _df = mo.sql(
        f"""
        CREATE INDEX IF NOT EXISTS gares_geom_index ON exclusion_rennes_metropole USING RTREE (geom)
        """,
        engine=conn
    )
    return


@app.cell
def _(conn, exclusion_rennes_metropole, mo):
    df_exclusion_zone = mo.sql(
        f"""
        SELECT
            ST_asText(geom) as geom_wkt
        FROM exclusion_rennes_metropole
        """,
        engine=conn
    )
    return (df_exclusion_zone,)


@app.cell
def _(df_exclusion_zone):
    df_exclusion_zone
    return


@app.cell
def _(mo):
    mo.md(r"""### Qui incite?""")
    return


@app.cell
def _(AOM_SIRET, df_journeys_raw, df_operators, pl):
    df_journeys_raw_wincentives = df_journeys_raw.with_columns(
        pl.col("incentive_sirets").list.contains(AOM_SIRET).alias("incentived_by_aom"),
        (
            pl.col("incentive_sirets")
            .list.set_intersection(df_operators["siret"].to_list())
            .list.len()
            > 0
        ).alias("incentived_by_operator"),
    )
    return (df_journeys_raw_wincentives,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""# Traitement Géo""")
    return


@app.cell
def _(df_journeys_raw_wincentives, pl, shapely):
    df_journeys_raw_wgeo = df_journeys_raw_wincentives.with_columns(
        pl.col("start_position")
        .map_elements(lambda x: shapely.from_wkb(x).wkt, return_dtype=pl.String)
        .alias("start_pos"),
        pl.col("end_position")
        .map_elements(lambda x: shapely.from_wkb(x).wkt, return_dtype=pl.String)
        .alias("end_pos"),
            pl.col("start_position")
        .map_elements(lambda x: shapely.from_wkb(x).y, return_dtype=pl.Float64)
        .alias("start_latitude"),
        pl.col("start_position")
        .map_elements(lambda x: shapely.from_wkb(x).x, return_dtype=pl.Float64)
        .alias("start_longitude"),
            pl.col("end_position")
        .map_elements(lambda x: shapely.from_wkb(x).y, return_dtype=pl.Float64)
        .alias("end_latitude"),
        pl.col("end_position")
        .map_elements(lambda x: shapely.from_wkb(x).x, return_dtype=pl.Float64)
        .alias("end_longitude"),
    )
    return (df_journeys_raw_wgeo,)


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""# Journeys raw vers duckdb""")
    return


@app.cell
def _(conn, df_journeys_raw_wgeo, mo):
    _df = mo.sql(
        f"""
        CREATE TABLE
          if NOT EXISTS journeys_raw AS
        SELECT
          _id,
          operator_id,
          operator_journey_id,
          operator_trip_id,
          driver_identity_key,
          first_trip_datetime,
          passenger_identity_key,
          passenger_first_trip_datetime,
          start_datetime,
          end_datetime,
          distance,
          driver_revenue,
          passenger_contribution,
          incentive_amount,
          amount_aom,
          incentive_sirets,
          start_position,
          end_position,
          passenger_seats,
          is_fully_inside_campaign_area,
          journey_line,
          start_com,
          end_com,
          incentived_by_aom,
          incentived_by_operator,
          ST_FlipCoordinates (ST_GeomFromText (start_pos)) AS start_pos,
          ST_FlipCoordinates (ST_GeomFromText (end_pos)) AS end_pos
        FROM
          df_journeys_raw_wgeo
        """,
        engine=conn
    )
    return (journeys_raw,)


@app.cell
def _(conn, mo):
    _df = mo.sql(
        f"""
        CREATE INDEX IF NOT EXISTS start_pos_idx ON journeys_raw USING RTREE (start_pos)
        """,
        engine=conn
    )
    return


@app.cell
def _(conn, mo):
    _df = mo.sql(
        f"""
        CREATE INDEX IF NOT EXISTS end_pos_idx ON journeys_raw USING RTREE (end_pos)
        """,
        engine=conn
    )
    return


@app.cell
def _(conn, journeys_raw, mo):
    n_journeys = mo.sql(
        f"""
        SELECT count(DISTINCT _id) FROM journeys_raw
        """,
        engine=conn
    )
    return (n_journeys,)


@app.cell
def _(conn, journeys_raw, mo):
    _df = mo.sql(
        f"""
        describe journeys_raw
        """,
        engine=conn
    )
    return


@app.cell
def _(n_journeys):
    n_journeys
    return


@app.cell
def _(conn, exclusion_rennes_metropole, journeys_raw, mo):
    df_excluded_journeys = mo.sql(
        f"""
        SELECT j.operator_journey_id, driver_revenue, passenger_contribution, incentive_amount, amount_aom, start_datetime, incentived_by_aom, incentived_by_operator FROM journeys_raw j join exclusion_rennes_metropole e on ST_Intersects(j.start_pos, e.geom) and  ST_Intersects(j.end_pos, e.geom)
        """,
        engine=conn
    )
    return (df_excluded_journeys,)


@app.cell
def _(conn, exclusion_rennes_metropole, journeys_raw, mo):
    _df = mo.sql(
        f"""
        SELECT count(DISTINCT j.operator_journey_id) FROM journeys_raw j join exclusion_rennes_metropole e on ST_Intersects(j.start_pos, e.geom) or  ST_Intersects(j.end_pos, e.geom)
        """,
        engine=conn
    )
    return


@app.cell
def _(ZoneInfo, datetime, df_journeys_raw_wgeo, pl):
    with pl.Config(set_fmt_str_lengths=120, set_tbl_width_chars=1000, set_tbl_rows=100):
        print(
            df_journeys_raw_wgeo.select(
                # Totaux généraux
                pl.col("_id").n_unique().alias("Nombre de trajets"),
                pl.col("_id")
                .filter(pl.col("incentive_amount") > 0)
                .n_unique()
                .alias("Nombre de trajets avec incitation"),
                (
                    100
                    * pl.col("_id").filter(pl.col("incentive_amount") > 0).n_unique()
                    / pl.col("_id").n_unique()
                ).alias("% trajets avec incitation"),
            
                pl.col("_id")
                .filter(pl.col("incentived_by_aom"))
                .n_unique()
                .alias("Nombre de trajets avec incitation AOM"),
                (
                    100
                    * pl.col("_id").filter(pl.col("incentived_by_aom")).n_unique()
                    / pl.col("_id").n_unique()
                ).alias("% trajets avec incitation AOM"),
            
                pl.col("_id")
                .filter(pl.col("incentived_by_operator"))
                .n_unique()
                .alias("Nombre de trajets avec incitation opérateur"),
                (
                    100
                    * pl.col("_id").filter(pl.col("incentived_by_operator")).n_unique()
                    / pl.col("_id").n_unique()
                ).alias("% trajets avec incitation opérateur"),
            
                pl.col("_id")
                .filter(pl.col("incentived_by_operator"), ~pl.col("incentived_by_aom"))
                .n_unique()
                .alias("Nombre de trajets avec incitation opérateur seule"),
                (
                    100.0
                    * pl.col("_id").filter((pl.col("incentived_by_operator")) & (~pl.col("incentived_by_aom"))).n_unique()
                    / pl.col("_id").n_unique()
                ).alias("% trajets avec incitation opérateur seule"),
            
                # Avant la campagne
                pl.col("_id")
                .filter(pl.col("start_datetime") < datetime(2025, 1, 1, tzinfo=ZoneInfo("GMT")))
                .n_unique()
                .alias("Nombre de trajets avant le début de la campagne"),
                pl.col("_id")
                .filter(
                    pl.col("incentive_amount") > 0,
                    pl.col("start_datetime") < datetime(2025, 1, 1, tzinfo=ZoneInfo("GMT"))
                )
                .n_unique()
                .alias("Nombre de trajets avec incitation avant le début de la campagne"),
                (
                    100.0
                    * pl.col("_id").filter(
                        pl.col("incentive_amount") > 0,
                        pl.col("start_datetime") < datetime(2025, 1, 1, tzinfo=ZoneInfo("GMT"))
                    ).n_unique()
                    / pl.col("_id").filter(pl.col("start_datetime") < datetime(2025, 1, 1, tzinfo=ZoneInfo("GMT"))).n_unique()
                ).alias("% trajets avec incitation avant le début de la campagne"),
            
                pl.col("_id")
                .filter(
                    pl.col("incentived_by_aom"),
                    pl.col("start_datetime") < datetime(2025, 1, 1, tzinfo=ZoneInfo("GMT"))
                )
                .n_unique()
                .alias("Nombre de trajets avec incitation AOM avant le début de la campagne"),
                (
                    100.0
                    * pl.col("_id").filter(
                        pl.col("incentived_by_aom"),
                        pl.col("start_datetime") < datetime(2025, 1, 1, tzinfo=ZoneInfo("GMT"))
                    ).n_unique()
                    / pl.col("_id").filter(pl.col("start_datetime") < datetime(2025, 1, 1, tzinfo=ZoneInfo("GMT"))).n_unique()
                ).alias("% trajets avec incitation AOM avant le début de la campagne"),
            
                pl.col("_id")
                .filter(
                    pl.col("incentived_by_operator"),
                    pl.col("start_datetime") < datetime(2025, 1, 1, tzinfo=ZoneInfo("GMT"))
                )
                .n_unique()
                .alias("Nombre de trajets avec incitation opérateur avant le début de la campagne"),
                (
                    100.0
                    * pl.col("_id").filter(
                        pl.col("incentived_by_operator"),
                        pl.col("start_datetime") < datetime(2025, 1, 1, tzinfo=ZoneInfo("GMT"))
                    ).n_unique()
                    / pl.col("_id").filter(pl.col("start_datetime") < datetime(2025, 1, 1, tzinfo=ZoneInfo("GMT"))).n_unique()
                ).alias("% trajets avec incitation opérateur avant le début de la campagne"),
            
                pl.col("_id")
                .filter(
                    pl.col("incentived_by_operator"),
                    ~pl.col("incentived_by_aom"),
                    pl.col("start_datetime") < datetime(2025, 1, 1, tzinfo=ZoneInfo("GMT"))
                )
                .n_unique()
                .alias("Nombre de trajets avec incitation opérateur seule avant le début de la campagne"),
                (
                    100.0
                    * pl.col("_id").filter(
                        pl.col("incentived_by_operator"),
                        ~pl.col("incentived_by_aom"),
                        pl.col("start_datetime") < datetime(2025, 1, 1, tzinfo=ZoneInfo("GMT"))
                    ).n_unique()
                    / pl.col("_id").filter(pl.col("start_datetime") < datetime(2025, 1, 1, tzinfo=ZoneInfo("GMT"))).n_unique()
                ).alias("% trajets avec incitation opérateur seule avant le début de la campagne"),
            
                # Durant la campagne
                pl.col("_id")
                .filter(pl.col("start_datetime") >= datetime(2025, 1, 1, tzinfo=ZoneInfo("GMT")))
                .n_unique()
                .alias("Nombre de trajets durant la campagne"),
                pl.col("_id")
                .filter(
                    pl.col("incentive_amount") > 0,
                    pl.col("start_datetime") >= datetime(2025, 1, 1, tzinfo=ZoneInfo("GMT"))
                )
                .n_unique()
                .alias("Nombre de trajets avec incitation durant la campagne"),
                (
                    100.0
                    * pl.col("_id").filter(
                        pl.col("incentive_amount") > 0,
                        pl.col("start_datetime") >= datetime(2025, 1, 1, tzinfo=ZoneInfo("GMT"))
                    ).n_unique()
                    / pl.col("_id").filter(pl.col("start_datetime") >= datetime(2025, 1, 1, tzinfo=ZoneInfo("GMT"))).n_unique()
                ).alias("% trajets avec incitation durant la campagne"),
            
                pl.col("_id")
                .filter(
                    pl.col("incentived_by_aom"),
                    pl.col("start_datetime") >= datetime(2025, 1, 1, tzinfo=ZoneInfo("GMT"))
                )
                .n_unique()
                .alias("Nombre de trajets avec incitation AOM durant la campagne"),
                (
                    100.0
                    * pl.col("_id").filter(
                        pl.col("incentived_by_aom"),
                        pl.col("start_datetime") >= datetime(2025, 1, 1, tzinfo=ZoneInfo("GMT"))
                    ).n_unique()
                    / pl.col("_id").filter(pl.col("start_datetime") >= datetime(2025, 1, 1, tzinfo=ZoneInfo("GMT"))).n_unique()
                ).alias("% trajets avec incitation AOM durant la campagne"),
            
                pl.col("_id")
                .filter(
                    pl.col("incentived_by_operator"),
                    pl.col("start_datetime") >= datetime(2025, 1, 1, tzinfo=ZoneInfo("GMT"))
                )
                .n_unique()
                .alias("Nombre de trajets avec incitation opérateur durant la campagne"),
                (
                    100.0
                    * pl.col("_id").filter(
                        pl.col("incentived_by_operator"),
                        pl.col("start_datetime") >= datetime(2025, 1, 1, tzinfo=ZoneInfo("GMT"))
                    ).n_unique()
                    / pl.col("_id").filter(pl.col("start_datetime") >= datetime(2025, 1, 1, tzinfo=ZoneInfo("GMT"))).n_unique()
                ).alias("% trajets avec incitation opérateur durant la campagne"),
            
                pl.col("_id")
                .filter(
                    pl.col("incentived_by_operator"),
                    ~pl.col("incentived_by_aom"),
                    pl.col("start_datetime") >= datetime(2025, 1, 1, tzinfo=ZoneInfo("GMT"))
                )
                .n_unique()
                .alias("Nombre de trajets avec incitation opérateur seule durant la campagne"),
                (
                    100.0
                    * pl.col("_id").filter(
                        pl.col("incentived_by_operator"),
                        ~pl.col("incentived_by_aom"),
                        pl.col("start_datetime") >= datetime(2025, 1, 1, tzinfo=ZoneInfo("GMT"))
                    ).n_unique()
                    / pl.col("_id").filter(pl.col("start_datetime") >= datetime(2025, 1, 1, tzinfo=ZoneInfo("GMT"))).n_unique()
                ).alias("% trajets avec incitation opérateur seule durant la campagne"),
            )
            .with_columns(pl.selectors.all().round(2))
            .unpivot()
        )
    return


@app.cell
def _(ZoneInfo, datetime, df_excluded_journeys, pl):
    with pl.Config(set_fmt_str_lengths=120, set_tbl_width_chars=1000, set_tbl_rows=100):
        print(
            df_excluded_journeys.select(
                # Totaux généraux
                pl.col("operator_journey_id").n_unique().alias("Nombre de trajets avec O/D dans la zone d'exclusion"),
            
                pl.col("operator_journey_id")
                .filter(pl.col("incentive_amount") > 0)
                .n_unique()
                .alias("Nombre de trajets avec incitation"),
                (
                    100
                    * pl.col("operator_journey_id").filter(pl.col("incentive_amount") > 0).n_unique()
                    / pl.col("operator_journey_id").n_unique()
                ).alias("% trajets avec incitation"),
            
                pl.col("operator_journey_id")
                .filter(pl.col("incentived_by_aom"))
                .n_unique()
                .alias("Nombre de trajets avec incitation AOM"),
                (
                    100
                    * pl.col("operator_journey_id").filter(pl.col("incentived_by_aom")).n_unique()
                    / pl.col("operator_journey_id").n_unique()
                ).alias("% trajets avec incitation AOM"),
            
                pl.col("operator_journey_id")
                .filter(pl.col("incentived_by_operator"))
                .n_unique()
                .alias("Nombre de trajets avec incitation opérateur"),
                (
                    100
                    * pl.col("operator_journey_id").filter(pl.col("incentived_by_operator")).n_unique()
                    / pl.col("operator_journey_id").n_unique()
                ).alias("% trajets avec incitation opérateur"),
            
                pl.col("operator_journey_id")
                .filter(pl.col("incentived_by_operator"), ~pl.col("incentived_by_aom"))
                .n_unique()
                .alias("Nombre de trajets avec incitation opérateur seule"),
                (
                    100.0
                    * pl.col("operator_journey_id").filter((pl.col("incentived_by_operator")) & (~pl.col("incentived_by_aom"))).n_unique()
                    / pl.col("operator_journey_id").n_unique()
                ).alias("% trajets avec incitation opérateur seule"),
            
                # Avant la campagne
                pl.col("operator_journey_id")
                .filter(pl.col("start_datetime") < datetime(2025, 1, 1, tzinfo=ZoneInfo("Europe/Paris")))
                .n_unique()
                .alias("Nombre de trajets avec O/D dans la zone d'exclusion avant le début de la campagne"),
            
                pl.col("operator_journey_id")
                .filter(
                    pl.col("incentive_amount") > 0,
                    pl.col("start_datetime") < datetime(2025, 1, 1, tzinfo=ZoneInfo("Europe/Paris"))
                )
                .n_unique()
                .alias("Nombre de trajets avec O/D dans la zone d'exclusion avec incitation avant le début de la campagne"),
                (
                    100.0
                    * pl.col("operator_journey_id").filter(
                        pl.col("incentive_amount") > 0,
                        pl.col("start_datetime") < datetime(2025, 1, 1, tzinfo=ZoneInfo("Europe/Paris"))
                    ).n_unique()
                    / pl.col("operator_journey_id").filter(pl.col("start_datetime") < datetime(2025, 1, 1, tzinfo=ZoneInfo("Europe/Paris"))).n_unique()
                ).alias("% trajets avec O/D dans la zone d'exclusion avec incitation avant le début de la campagne"),
            
                pl.col("operator_journey_id")
                .filter(
                    pl.col("incentived_by_aom"),
                    pl.col("start_datetime") < datetime(2025, 1, 1, tzinfo=ZoneInfo("Europe/Paris"))
                ) 
                .n_unique()
                .alias("Nombre de trajets avec O/D dans la zone d'exclusion avec incitation AOM avant le début de la campagne"),
                (
                    100.0
                    * pl.col("operator_journey_id").filter(
                        pl.col("incentived_by_aom"),
                        pl.col("start_datetime") < datetime(2025, 1, 1, tzinfo=ZoneInfo("Europe/Paris"))
                    ).n_unique()
                    / pl.col("operator_journey_id").filter(pl.col("start_datetime") < datetime(2025, 1, 1, tzinfo=ZoneInfo("Europe/Paris"))).n_unique()
                ).alias("% trajets avec O/D dans la zone d'exclusion avec incitation AOM avant le début de la campagne"),
            
                pl.col("operator_journey_id")
                .filter(
                    pl.col("incentived_by_operator"),
                    pl.col("start_datetime") < datetime(2025, 1, 1, tzinfo=ZoneInfo("Europe/Paris"))
                )
                .n_unique()
                .alias("Nombre de trajets avec O/D dans la zone d'exclusion avec incitation opérateur avant le début de la campagne"),
                (
                    100.0
                    * pl.col("operator_journey_id").filter(
                        pl.col("incentived_by_operator"),
                        pl.col("start_datetime") < datetime(2025, 1, 1, tzinfo=ZoneInfo("Europe/Paris"))
                    ).n_unique()
                    / pl.col("operator_journey_id").filter(pl.col("start_datetime") < datetime(2025, 1, 1, tzinfo=ZoneInfo("Europe/Paris"))).n_unique()
                ).alias("% trajets avec O/D dans la zone d'exclusion avec incitation opérateur avant le début de la campagne"),
            
                pl.col("operator_journey_id")
                .filter(
                    pl.col("incentived_by_operator"),
                    ~pl.col("incentived_by_aom"),
                    pl.col("start_datetime") < datetime(2025, 1, 1, tzinfo=ZoneInfo("Europe/Paris"))
                )
                .n_unique()
                .alias("Nombre de trajets avec O/D dans la zone d'exclusion avec incitation opérateur seule avant le début de la campagne"),
                (
                    100.0
                    * pl.col("operator_journey_id").filter(
                        pl.col("incentived_by_operator"),
                        ~pl.col("incentived_by_aom"),
                        pl.col("start_datetime") < datetime(2025, 1, 1, tzinfo=ZoneInfo("Europe/Paris"))
                    ).n_unique()
                    / pl.col("operator_journey_id").filter(pl.col("start_datetime") < datetime(2025, 1, 1, tzinfo=ZoneInfo("Europe/Paris"))).n_unique()
                ).alias("% trajets avec O/D dans la zone d'exclusion avec incitation opérateur seule avant le début de la campagne"),
            
                # Durant la campagne
                pl.col("operator_journey_id")
                .filter(pl.col("start_datetime") >= datetime(2025, 1, 1, tzinfo=ZoneInfo("Europe/Paris")))
                .n_unique()
                .alias("Nombre de trajets avec O/D dans la zone d'exclusion durant la campagne"),
            
                pl.col("operator_journey_id")
                .filter(
                    pl.col("incentive_amount") > 0,
                    pl.col("start_datetime") >= datetime(2025, 1, 1, tzinfo=ZoneInfo("Europe/Paris"))
                )
                .n_unique()
                .alias("Nombre de trajets avec O/D dans la zone d'exclusion avec incitation durant la campagne"),
                (
                    100.0
                    * pl.col("operator_journey_id").filter(
                        pl.col("incentive_amount") > 0,
                        pl.col("start_datetime") >= datetime(2025, 1, 1, tzinfo=ZoneInfo("Europe/Paris"))
                    ).n_unique()
                    / pl.col("operator_journey_id").filter(pl.col("start_datetime") >= datetime(2025, 1, 1, tzinfo=ZoneInfo("Europe/Paris"))).n_unique()
                ).alias("% trajets avec O/D dans la zone d'exclusion avec incitation durant la campagne"),
            
                pl.col("operator_journey_id")
                .filter(
                    pl.col("incentived_by_aom"),
                    pl.col("start_datetime") >= datetime(2025, 1, 1, tzinfo=ZoneInfo("Europe/Paris"))
                ) 
                .n_unique()
                .alias("Nombre de trajets avec O/D dans la zone d'exclusion avec incitation AOM durant la campagne"),
                (
                    100.0
                    * pl.col("operator_journey_id").filter(
                        pl.col("incentived_by_aom"),
                        pl.col("start_datetime") >= datetime(2025, 1, 1, tzinfo=ZoneInfo("Europe/Paris"))
                    ).n_unique()
                    / pl.col("operator_journey_id").filter(pl.col("start_datetime") >= datetime(2025, 1, 1, tzinfo=ZoneInfo("Europe/Paris"))).n_unique()
                ).alias("% trajets avec O/D dans la zone d'exclusion avec incitation AOM durant la campagne"),
            
                pl.col("operator_journey_id")
                .filter(
                    pl.col("incentived_by_operator"),
                    pl.col("start_datetime") >= datetime(2025, 1, 1, tzinfo=ZoneInfo("Europe/Paris"))
                )
                .n_unique()
                .alias("Nombre de trajets avec O/D dans la zone d'exclusion avec incitation opérateur durant la campagne"),
                (
                    100.0
                    * pl.col("operator_journey_id").filter(
                        pl.col("incentived_by_operator"),
                        pl.col("start_datetime") >= datetime(2025, 1, 1, tzinfo=ZoneInfo("Europe/Paris"))
                    ).n_unique()
                    / pl.col("operator_journey_id").filter(pl.col("start_datetime") >= datetime(2025, 1, 1, tzinfo=ZoneInfo("Europe/Paris"))).n_unique()
                ).alias("% trajets avec O/D dans la zone d'exclusion avec incitation opérateur durant la campagne"),
            
                pl.col("operator_journey_id")
                .filter(
                    pl.col("incentived_by_operator"),
                    ~pl.col("incentived_by_aom"),
                    pl.col("start_datetime") >= datetime(2025, 1, 1, tzinfo=ZoneInfo("Europe/Paris"))
                )
                .n_unique()
                .alias("Nombre de trajets avec O/D dans la zone d'exclusion avec incitation opérateur seule durant la campagne"),
                (
                    100.0
                    * pl.col("operator_journey_id").filter(
                        pl.col("incentived_by_operator"),
                        ~pl.col("incentived_by_aom"),
                        pl.col("start_datetime") >= datetime(2025, 1, 1, tzinfo=ZoneInfo("Europe/Paris"))
                    ).n_unique()
                    / pl.col("operator_journey_id").filter(pl.col("start_datetime") >= datetime(2025, 1, 1, tzinfo=ZoneInfo("Europe/Paris"))).n_unique()
                ).alias("% trajets avec O/D dans la zone d'exclusion avec incitation opérateur seule durant la campagne"),
            )
            .with_columns(pl.selectors.all().round(2))
            .unpivot()
        )
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md(r"""# Carto""")
    return


@app.cell
def _(pl, plh3):
    def pl_add_h3cells(df_journeys_raw: pl.DataFrame, end_or_start: str):
        """Compte les trajets uniques par cellule H3"""
        df_h3 = (
            df_journeys_raw.with_columns(
                plh3.latlng_to_cell(
                    lat=f"{end_or_start}_latitude", 
                    lng=f"{end_or_start}_longitude", 
                    resolution=9
                ).alias("h3_cell"),
            )
            .group_by("h3_cell")
            .agg(pl.col("_id").n_unique().alias(f"num_{end_or_start}s"))
        )
        return df_h3
    return (pl_add_h3cells,)


@app.cell
def _(
    OUTPUT_PATH,
    RENNES_EXCLUSION_GEOJSON,
    bcm,
    cm,
    df_journeys_raw_wgeo,
    folium,
    gpd,
    mcolors,
    pl,
    pl_add_h3cells,
    plh3,
):
    from shapely.geometry import Polygon
    import pandas as pd
    def create_density_map(df_journeys_raw: pl.DataFrame, exclusion_geojson_path: str):
        """Crée une carte de densité H3 avec zone d'exclusion"""

        # Compter les départs et arrivées par cellule H3
        df_h3_starts = pl_add_h3cells(df_journeys_raw, "start")
        df_h3_ends = pl_add_h3cells(df_journeys_raw, "end")

        # Fusionner et sommer les départs et arrivées
        df_h3 = (
            df_h3_starts
            .join(df_h3_ends, on="h3_cell", how="full", validate="1:1")
            .with_columns([
                # Unifier les colonnes après le full join
                pl.coalesce(["h3_cell", "h3_cell_right"]).alias("cell_joined"),
                # Remplir les NULL et caster en Int64
                pl.col("num_starts").fill_null(0).cast(pl.Int64).alias("num_starts"),
                pl.col("num_ends").fill_null(0).cast(pl.Int64).alias("num_ends"),
            ])
            .with_columns([
                # Calculer le total
                (pl.col("num_starts") + pl.col("num_ends")).alias("num_total"),
                # Créer la géométrie
                plh3.cell_to_boundary(pl.col("cell_joined")).alias("cell_geom"),
            ])
            .with_columns([
                # Convertir en Polygon Shapely (inverser lat/lon -> lon/lat)
                pl.col("cell_geom").map_elements(
                    lambda x: Polygon([[e[1], e[0]] for e in x]),
                    return_dtype=pl.Object
                ).alias("cell_geom"),
                pl.col("cell_joined").cast(pl.String),
            ])
            .drop(["h3_cell", "h3_cell_right"])
        )

        print(df_h3.head())

        # Créer le GeoDataFrame
        gdf = gpd.GeoDataFrame(
            df_h3.to_pandas(),
            geometry="cell_geom",
            crs=4326
        )

        # Charger la zone d'exclusion
        gdf_exclusion = gpd.read_file(exclusion_geojson_path)
        if gdf_exclusion.crs != 4326:
            gdf_exclusion = gdf_exclusion.to_crs(4326)
        for col in gdf_exclusion.columns:
            if pd.api.types.is_datetime64_any_dtype(gdf_exclusion[col]):
                gdf_exclusion[col] = gdf_exclusion[col].astype(str)

        # Calculer le centre de la carte
        center = gdf.geometry.unary_union.centroid.coords[0][::-1]  # (lat, lon)

        # Colormap pour le nombre total de trajets
        vmax = gdf["num_total"].max()
        cmap = cm.get_cmap("YlOrRd")
        color_scale = bcm.LinearColormap(
            colors=[mcolors.to_hex(cmap(i)) for i in [0.0, 0.25, 0.5, 0.75, 1.0]],
            vmin=0,
            vmax=vmax,
        )
        color_scale.caption = "Nombre total de trajets (départs + arrivées)"

        m = folium.Map(location=center, zoom_start=9, tiles="openstreetmap")

        # Ajouter les cellules H3
        folium.GeoJson(
            gdf.to_json(),
            style_function=lambda feature: {
                "fillColor": color_scale(feature["properties"]["num_total"]),
                "color": "black",
                "weight": 0.5,
                "fillOpacity": 0.6,
            },
            tooltip=folium.GeoJsonTooltip(
                fields=["cell_joined", "num_starts", "num_ends", "num_total"],
                aliases=["Cellule H3", "Départs", "Arrivées", "Total"],
                localize=True,
            ),
        ).add_to(m)

        # Ajouter la zone d'exclusion
        folium.GeoJson(
            gdf_exclusion.to_json(),
            style_function=lambda feature: {
                "fill": False,
                "color": "darkred",
                "weight": 3,
            },
            name="Zone d'exclusion Rennes Métropole",
        ).add_to(m)

        # Ajout de la colorbar et contrôle des couches
        color_scale.add_to(m)
        folium.LayerControl().add_to(m)

        return m


    # Exécution
    density_map = create_density_map(
        df_journeys_raw_wgeo, 
        exclusion_geojson_path=RENNES_EXCLUSION_GEOJSON
    )
    density_map.save(OUTPUT_PATH / "density_h3.html")
    return


@app.cell
def _():
    return


if __name__ == "__main__":
    app.run()
