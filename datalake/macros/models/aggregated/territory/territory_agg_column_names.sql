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
    'carpools_new_passengers',
    'unique_passengers',
    'new_passengers',
    'passenger_seats',
    'passenger_over_18',
    'passenger_contribution',
    'distance',
    'duration',
    'oi_collectivite',
    'oi_operator',
    'oi_other',
    'no_oi',
    'oi_amount_collectivite',
    'oi_amount_operator',
    'oi_amount_other',
    'oi_amount_total',
    'ci_collectivite',
    'ci_amount_total',
    'ci_result_total',
    'hours_distribution',
    'dist_distribution',
    'oc_distribution'
  ]) %}
{% endmacro %}
