{% macro direction_agg_columns() %}
  COUNT(*) AS carpools,
  COUNT(*) FILTER (WHERE is_intra) AS intra_carpools,
  COUNT(*) FILTER (WHERE is_new_driver) AS carpools_new_drivers,
  COUNT(*) FILTER (WHERE is_new_passenger) AS carpools_new_passengers,
  COUNT(DISTINCT operator_trip_id) AS trips,
  COUNT(DISTINCT driver_key) AS unique_drivers,
  COUNT(DISTINCT passenger_key) AS unique_passengers,
  COUNT(DISTINCT driver_key) FILTER (WHERE is_new_driver) AS new_drivers,
  COUNT(DISTINCT passenger_key) FILTER (WHERE is_new_passenger) AS new_passengers,
  SUM(passenger_seats) AS passenger_seats,
  SUM(distance) AS distance,
  SUM(oi_collectivite) AS incentive_collectivite,
  SUM(oi_operator) AS incentive_operator,
  SUM(oi_other) AS incentive_other,
  COUNT(*) FILTER (WHERE NOT with_incentive) AS no_incentive,
  ARRAY[
    {% for h in range(24) %}
      COUNT(*) FILTER (WHERE hour = {{h}}){% if not loop.last %},{% endif %}
    {% endfor %}
  ] AS hours_distribution,
  ARRAY[
    COUNT(*) FILTER (WHERE dist_class = '0-10'),
    COUNT(*) FILTER (WHERE dist_class = '10-20'),
    COUNT(*) FILTER (WHERE dist_class = '20-30'),
    COUNT(*) FILTER (WHERE dist_class = '30-40'),
    COUNT(*) FILTER (WHERE dist_class = '40-50'),
    COUNT(*) FILTER (WHERE dist_class = '>50')
  ] AS dist_distribution
{% endmacro %}