{% macro territory_agg_columns() %}
  COUNT(*) AS carpools,
  COUNT(*) FILTER (WHERE is_intra) AS intra_carpools,
  COUNT(DISTINCT operator_trip_id) AS trips, 
  COUNT(*) FILTER (WHERE is_new_driver) AS carpools_new_drivers,
  COUNT(DISTINCT driver_key) AS unique_drivers,
  COUNT(DISTINCT driver_key) FILTER (WHERE is_new_driver) AS new_drivers,
  SUM(driver_revenue) AS driver_revenue,
  COUNT(*) FILTER (WHERE is_new_passenger) AS carpools_new_passengers,
  COUNT(DISTINCT passenger_key) AS unique_passengers,
  COUNT(DISTINCT passenger_key) FILTER (WHERE is_new_passenger) AS new_passengers,
  SUM(passenger_seats) AS passenger_seats,
  COUNT(*) FILTER (WHERE passenger_over_18) AS passenger_over_18,
  SUM(passenger_contribution) AS passenger_contribution,
  SUM(distance) AS distance,
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
    COUNT(*) FILTER (WHERE dist_class = '00-05'),
    COUNT(*) FILTER (WHERE dist_class = '05-10'),
    COUNT(*) FILTER (WHERE dist_class = '10-15'),
    COUNT(*) FILTER (WHERE dist_class = '15-20'),
    COUNT(*) FILTER (WHERE dist_class = '20-25'),
    COUNT(*) FILTER (WHERE dist_class = '25-30'),
    COUNT(*) FILTER (WHERE dist_class = '30-35'),
    COUNT(*) FILTER (WHERE dist_class = '35-40'),
    COUNT(*) FILTER (WHERE dist_class = '40-45'),
    COUNT(*) FILTER (WHERE dist_class = '45-50'),
    COUNT(*) FILTER (WHERE dist_class = '50-55'),
    COUNT(*) FILTER (WHERE dist_class = '55-60'),
    COUNT(*) FILTER (WHERE dist_class = '60-65'),
    COUNT(*) FILTER (WHERE dist_class = '65-70'),
    COUNT(*) FILTER (WHERE dist_class = '70-75'),
    COUNT(*) FILTER (WHERE dist_class = '75-80'),
    COUNT(*) FILTER (WHERE dist_class = '80+')
  ] AS dist_distribution,
  ARRAY[
    COUNT(*) FILTER (WHERE operator_class = 'A'),
    COUNT(*) FILTER (WHERE operator_class = 'B'),
    COUNT(*) FILTER (WHERE operator_class = 'C')
  ] AS oc_distribution

{% endmacro %}