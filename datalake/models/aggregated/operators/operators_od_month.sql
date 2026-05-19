{{
  config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key=['operator_id', 'incremental_date', 'start_code', 'end_code'],
    indexes=[
      { 'columns': ['operator_id'] },
      { 'columns': ['incremental_date'] },
      { 'columns': ['start_code', 'end_code'] }
    ],
    tags=['aggregated', 'operators', 'od', 'daily']
  )
}}

WITH daily AS (
  SELECT *
  FROM {{ ref('operators_od_day') }}
  WHERE {{ time_filter('incremental_date', type='timestamp', default_start="'2020-01-01 00:00:00'", lookback_nb=1, lookback_unit='month') }}
)

SELECT
  operator_id,
  operator_name,
  start_code,
  end_code,
  {{ incremental_columns('incremental_date', 'month') }},
  SUM(carpools) AS carpools,
  SUM(trips) AS trips,
  SUM(carpools_new_drivers) AS carpools_new_drivers,
  SUM(unique_drivers) AS unique_drivers,
  SUM(new_drivers) AS new_drivers,
  SUM(driver_revenue) AS driver_revenue,
  SUM(carpools_new_passengers) AS carpools_new_passengers,
  SUM(unique_passengers) AS unique_passengers,
  SUM(new_passengers) AS new_passengers,
  SUM(passenger_seats) AS passenger_seats,
  SUM(passenger_over_18) AS passenger_over_18,
  SUM(passenger_contribution) AS passenger_contribution,
  SUM(distance) AS distance,
  SUM(oi_collectivite) AS oi_collectivite,
  SUM(oi_operator) AS oi_operator,
  SUM(oi_other) AS oi_other,
  SUM(no_oi) AS no_oi,
  SUM(oi_amount_collectivite) AS oi_amount_collectivite,
  SUM(oi_amount_operator) AS oi_amount_operator,
  SUM(oi_amount_other) AS oi_amount_other,
  SUM(oi_amount_total) AS oi_amount_total,
  SUM(ci_collectivite) AS ci_collectivite,
  SUM(ci_amount_total) AS ci_amount_total,
  SUM(ci_result_total) AS ci_result_total,
  ARRAY[
    {% for h in range(24) %}
      SUM(hours_distribution[{{ h + 1 }}]){% if not loop.last %},{% endif %}
    {% endfor %}
  ] AS hours_distribution,
  ARRAY[
    SUM(oc_distribution[1]),
    SUM(oc_distribution[2]),
    SUM(oc_distribution[3])
  ] AS oc_distribution
FROM daily
GROUP BY 1, 2, 3, 4, {{ group_by_grain('month', 5) }}