{# with_incentive_split: percentile_cont et le NOT EXISTS sur oi_details ne sont
   consommes que par mb_aom_monthly_carpools (aom/aomreg) - desactives ailleurs
   (h3z8/h3z9 en tete) pour eviter des requetes couteuses. #}
{% macro territory_agg_columns(with_incentive_split=false) %}
  COUNT(*) AS carpools,
  COUNT(*) FILTER (WHERE is_intra) AS intra_carpools,
  COUNT(DISTINCT operator_trip_id) AS trips, 
  COUNT(*) FILTER (WHERE is_new_driver) AS carpools_new_drivers,
  COUNT(DISTINCT driver_key) AS unique_drivers,
  COUNT(DISTINCT driver_key) FILTER (WHERE is_new_driver) AS new_drivers,
  SUM(driver_revenue) AS driver_revenue,
  SUM(driver_revenue) FILTER (WHERE COALESCE(is_intra, false)) AS driver_revenue_intra,
  SUM(driver_revenue) FILTER (WHERE NOT COALESCE(is_intra, false)) AS driver_revenue_inter,
  COUNT(*) FILTER (WHERE is_new_passenger) AS carpools_new_passengers,
  COUNT(DISTINCT passenger_key) AS unique_passengers,
  COUNT(DISTINCT passenger_key) FILTER (WHERE is_new_passenger) AS new_passengers,
  SUM(passenger_seats) AS passenger_seats,
  COUNT(*) FILTER (WHERE passenger_over_18) AS passenger_over_18,
  SUM(passenger_contribution) AS passenger_contribution,
  SUM(passenger_contribution) FILTER (WHERE COALESCE(is_intra, false)) AS passenger_contribution_intra,
  SUM(passenger_contribution) FILTER (WHERE NOT COALESCE(is_intra, false)) AS passenger_contribution_inter,
  SUM(distance) AS distance,
  SUM(distance) FILTER (WHERE COALESCE(is_intra, false)) AS distance_intra,
  SUM(distance) FILTER (WHERE NOT COALESCE(is_intra, false)) AS distance_inter,
  AVG(distance) AS mean_distance,
  {% if with_incentive_split %}
  percentile_cont(0.5) WITHIN GROUP (ORDER BY distance) as median_distance,
  percentile_cont(0.25) WITHIN GROUP (ORDER BY distance) as q1_distance,
  percentile_cont(0.75) WITHIN GROUP (ORDER BY distance) as q3_distance,
  {% else %}
  NULL::numeric as median_distance,
  NULL::numeric as q1_distance,
  NULL::numeric as q3_distance,
  {% endif %}
  SUM(duration) AS duration,
  SUM(oi_collectivite) AS oi_collectivite,
  COUNT(*) FILTER (WHERE oi_operator > 0) AS oi_operator,
  COUNT(*) FILTER (WHERE oi_other > 0) AS oi_other,
  {% if with_incentive_split %}
  COUNT(*) FILTER (
    WHERE oi_details IS NOT NULL
      AND jsonb_array_length(oi_details) > 0
      AND NOT EXISTS (
        SELECT 1 FROM jsonb_array_elements(oi_details) elem
        WHERE elem ->> 'type' != 'operator'
      )
  ) AS oi_operator_only,
  {% else %}
  NULL::bigint AS oi_operator_only,
  {% endif %}
  COUNT(*) FILTER (WHERE NOT with_incentive) AS no_oi,
  SUM(oi_amount_collectivite) AS oi_amount_collectivite,
  SUM(oi_amount_operator) AS oi_amount_operator,
  SUM(oi_amount_operator) FILTER (WHERE COALESCE(is_intra, false)) AS oi_amount_operator_intra,
  SUM(oi_amount_operator) FILTER (WHERE NOT COALESCE(is_intra, false)) AS oi_amount_operator_inter,
  SUM(oi_amount_other) AS oi_amount_other,
  SUM(oi_amount_total) AS oi_amount_total,
  SUM(oi_amount_total) FILTER (WHERE COALESCE(is_intra, false)) AS oi_amount_total_intra,
  SUM(oi_amount_total) FILTER (WHERE NOT COALESCE(is_intra, false)) AS oi_amount_total_inter,
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