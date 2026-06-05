{% macro user_agg_columns() %}
  COUNT(*) AS carpools,
  COUNT(DISTINCT operator_trip_id) AS trips,
  SUM(distance) AS distance,
  SUM(duration) AS duration,
  SUM(driver_revenue) AS driver_revenue,
  SUM(passenger_seats) AS passenger_seats,
  COUNT(*) FILTER (WHERE passenger_over_18) AS passenger_over_18,
  SUM(passenger_contribution) AS passenger_contribution,
  SUM(oi_collectivite) AS oi_collectivite,
  SUM(oi_operator) AS oi_operator,
  SUM(oi_other) AS oi_other,
  COUNT(*) FILTER (WHERE NOT with_incentive) AS no_oi,
  SUM(oi_amount_collectivite) AS oi_amount_collectivite,
  SUM(oi_amount_operator) AS oi_amount_operator,
  SUM(oi_amount_other) AS oi_amount_other,
  SUM(oi_amount_total) AS oi_amount_total,
  SUM(
    jsonb_array_length(
      COALESCE(campaigns, '[]'::jsonb)
    )
  ) AS ci_collectivite,
  SUM(campaigns_amount_total) AS ci_amount_total,
  SUM(campaigns_result_total) AS ci_result_total,
  ARRAY[
    {% for h in range(24) %}
      COUNT(*) FILTER (WHERE hour = {{h}}){% if not loop.last %},{% endif %}
    {% endfor %}
  ] AS hours_distribution,
  ARRAY[
    COUNT(*) FILTER (WHERE operator_class = 'A'),
    COUNT(*) FILTER (WHERE operator_class = 'B'),
    COUNT(*) FILTER (WHERE operator_class = 'C')
  ] AS oc_distribution
{% endmacro %}
