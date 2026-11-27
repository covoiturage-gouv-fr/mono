{{ config(
  materialized='view',
  tags=['app_metabase', 'territory_monthly_financials']
) }}

{% for perim in mb_territory_perims() %}
  SELECT
    '{{ perim }}'
      AS perim,
    code,
    to_char(incremental_date, 'YYYY-MM')
      AS date,
    carpools,
    oi_amount_total,
    oi_amount_collectivite,
    oi_amount_operator,
    oi_amount_other,
    round(driver_revenue::numeric / nullif(carpools, 0), 2)
      AS avg_driver_revenue,
    round(passenger_contribution::numeric / nullif(carpools, 0), 2)
      AS avg_passenger_contribution,
    round(oi_amount_total / nullif(carpools, 0), 2)
      AS avg_incentive,
    round(oi_amount_collectivite / nullif(carpools, 0), 2)
      AS avg_incentive_collectivite,
    round(oi_amount_operator / nullif(carpools, 0), 2)
      AS avg_incentive_operator
  FROM {{ ref('territory_month_' ~ perim ~ '_both') }}
  {% if not loop.last %}UNION ALL{% endif %}
{% endfor %}
