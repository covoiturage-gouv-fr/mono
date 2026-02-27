# macros/get_latest_millesime_or.py
from sqlmesh import macro
from sqlglot import exp

@macro()
def join_perimeters_agg(evaluator, start_ts, end_ts):
    return exp.maybe_parse(f"""
        (
            SELECT DISTINCT ON (p.code, p.type, y.year)
                p.code,
                p.type,
                p.libelle,
                p.centroid,
                p.year,
                y.year AS j_year
            FROM trusted_zone.perimeters_agg p
            CROSS JOIN (
                SELECT EXTRACT(YEAR FROM {start_ts}::date)::int AS year
                UNION
                SELECT EXTRACT(YEAR FROM {end_ts}::date)::int
            ) y
            WHERE p.year <= y.year
            ORDER BY p.code, p.type, y.year, p.year DESC
        )
    """)