from sqlmesh import macro


def build_incentives_query(start_ts, end_ts) -> str:
    """Build the incentives SELECT query for a given date range.

    Queries policy.incentives and related source tables directly.
    Can be imported by Python models/utilities or called as a SQLMesh macro
    with @campaign_incentives(@start_ts, @end_ts) in SQL models.
    """
    return f"""
WITH ni AS (
  SELECT
    pi._id,
    c2._id AS carpool_v2_id,
    c2.operator_id,
    c2.operator_journey_id
  FROM policy.incentives pi
  LEFT JOIN carpool.carpools c1 ON pi.carpool_id = c1._id
  LEFT JOIN carpool_v2.carpools c2 ON c1.acquisition_id = c2.legacy_id
  WHERE pi.carpool_id IS NOT NULL
    AND pi.datetime BETWEEN '{start_ts}' AND '{end_ts}'

  UNION

  SELECT
    pi._id,
    c2._id AS carpool_v2_id,
    pi.operator_id,
    pi.operator_journey_id
  FROM policy.incentives pi
  LEFT JOIN carpool_v2.carpools c2
    ON pi.operator_id = c2.operator_id AND pi.operator_journey_id = c2.operator_journey_id
  WHERE pi.operator_id IS NOT NULL AND pi.operator_journey_id IS NOT NULL
    AND pi.datetime BETWEEN '{start_ts}' AND '{end_ts}'
)

SELECT
  pi._id,
  ni.carpool_v2_id,
  pi.datetime,
  ni.operator_id,
  ni.operator_journey_id,
  pi.policy_id AS campaign_id,
  pp.name AS campaign_name,
  ccp.siret AS territory_siret,
  ttg.name AS territory_name,
  pi.amount,
  pi.result,
  pi.status::VARCHAR AS status,
  pi.state::VARCHAR AS state

FROM policy.incentives pi
JOIN ni                                  ON pi._id = ni._id
LEFT JOIN policy.policies pp             ON pi.policy_id    = pp._id
LEFT JOIN territory.territory_group ttg  ON pp.territory_id = ttg._id
LEFT JOIN company.companies ccp          ON ttg.company_id  = ccp._id

ORDER BY pi.datetime
"""


@macro()
def campaign_incentives(evaluator, start_ts, end_ts):
    """SQLMesh macro wrapper around build_incentives_query.

    Usage in SQL models:
        @campaign_incentives(@start_ts, @end_ts)
    """
    return build_incentives_query(str(start_ts).strip("'"), str(end_ts).strip("'"))
