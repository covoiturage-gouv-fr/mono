{{ config(
  materialized='view',
  tags=['app_metabase', 'territory_monthly_carpools']
) }}

{% for perim in mb_territory_perims() %}
  SELECT
    '{{ perim }}'
      AS perim,
    t.code,
    to_char(t.incremental_date, 'YYYY-MM')
      AS date,
    t.carpools,
    t.carpools - t.no_oi
      AS carpools_with_incentive,
    round(100.0 * (t.carpools - t.no_oi) / nullif(t.carpools, 0), 1)
      AS incited_pct,
    i.carpools_collectivite_incentive
      AS carpools_with_collectivite_incentive,
    i.carpools_operator_incentive
      AS carpools_with_operator_incentive,
    i.carpools_operator_only_incentive
      AS carpools_with_operator_incentive_only,
    i.carpools_other_incentive
      AS carpools_with_other_incentive,
    t.intra_carpools,
    t.carpools - t.intra_carpools
      AS inter_carpools
  FROM {{ ref('territory_month_' ~ perim ~ '_both') }} AS t
  LEFT JOIN {{ ref('incited_carpools_month_' ~ perim ~ '_both') }} AS i
    ON t.code = i.code AND t.incremental_date = i.incremental_date
  {% if not loop.last %}UNION ALL{% endif %}
{% endfor %}
