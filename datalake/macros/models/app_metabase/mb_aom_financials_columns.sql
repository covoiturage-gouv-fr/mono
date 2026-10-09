{# Colonnes communes aux 4 blocs aom/aomreg x weekly/monthly de mb_aom_weekly_financials
    macro pour factoriser et éviter les divergences
  #}
{% macro mb_aom_financials_columns() %}
  {% do return([
    'carpools',
    'intra_carpools',
    'carpools - intra_carpools AS inter_carpools',
    'driver_revenue',
    'driver_revenue_intra',
    'driver_revenue_inter',
    'passenger_contribution',
    'passenger_contribution_intra',
    'passenger_contribution_inter',
    'oi_amount_total',
    'oi_amount_total_intra',
    'oi_amount_total_inter',
    'oi_amount_collectivite_self',
    'oi_amount_collectivite_self_intra',
    'oi_amount_collectivite_self_inter',
    'oi_amount_collectivite_other',
    'oi_amount_operator',
    'oi_amount_operator_intra',
    'oi_amount_operator_inter',
    'ci_amount_total_self',
    'ci_amount_total_other',
    'ci_result_total_self',
    'ci_result_total_other'
  ]) %}
{% endmacro %}
