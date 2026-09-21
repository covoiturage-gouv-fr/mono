{# arr et plm sont full-refreshés/altérés indépendamment : leur ordre physique de colonnes
   peut diverger. On énumère les colonnes par nom pour que le UNION ALL apparie par nom,
   pas par position.

   Doit rester synchro a la main avec les alias de territory_agg_columns() : verifie par
   tests/aggregated/territory_day_arr_both/column_names_macro_sync.sql. #}
{% macro territory_agg_column_names() %}
  {% do return([
    'carpools',
    'intra_carpools',
    'trips',
    'carpools_new_drivers',
    'unique_drivers',
    'new_drivers',
    'driver_revenue',
    'driver_revenue_intra',
    'driver_revenue_inter',
    'carpools_new_passengers',
    'unique_passengers',
    'new_passengers',
    'passenger_seats',
    'passenger_over_18',
    'passenger_contribution',
    'passenger_contribution_intra',
    'passenger_contribution_inter',
    'distance',
    'distance_intra',
    'distance_inter',
    'mean_distance',
    'median_distance',
    'q1_distance',
    'q3_distance',
    'duration',
    'oi_collectivite',
    'oi_operator',
    'oi_other',
    'oi_operator_only',
    'no_oi',
    'oi_amount_collectivite',
    'oi_amount_operator',
    'oi_amount_operator_intra',
    'oi_amount_operator_inter',
    'oi_amount_other',
    'oi_amount_total',
    'oi_amount_total_intra',
    'oi_amount_total_inter',
    'ci_collectivite',
    'ci_amount_total',
    'ci_result_total',
    'hours_distribution',
    'dist_distribution',
    'oc_distribution'
  ]) %}
{% endmacro %}
