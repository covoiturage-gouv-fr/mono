{% macro filtered_carpools(
  perim='arr',
  column='c.start_datetime_tz',
  model_column='incremental_date',
  type='timestamp',
  default_start="'2020-01-01 00:00:00'",
  lookback_nb=0,
  lookback_unit='day'
) %}
  
  {%- set perim_join = carpools_perim_join() -%}
  {%- set aom_region_ref = ref('aom_region') -%}
 
  {% set perimeters = {
    'arr': {
      'start_col':  'c.start_geo_code',
      'end_col':    'c.end_geo_code',
      'joins':      '',
      'is_intra':   'c.is_intra',
      'where_geo':  "(c.start_geo_code NOT IN ('75056', '13055', '69123') OR c.end_geo_code NOT IN ('75056', '13055', '69123'))"
    },
    'h3z9': {
      'start_col':  'c.start_h3index_z9',
      'end_col':    'c.end_h3index_z9',
      'joins':      '',
      'is_intra':   'c.is_intra',
      'where_geo':  '(c.start_h3index_z9 IS NOT NULL OR c.end_h3index_z9 IS NOT NULL)'
    },
    'h3z8': {
      'start_col':  'c.start_h3index_z8',
      'end_col':    'c.end_h3index_z8',
      'joins':      '',
      'is_intra':   'c.is_intra',
      'where_geo':  '(c.start_h3index_z8 IS NOT NULL OR c.end_h3index_z8 IS NOT NULL)'
    },
    'dep': {
      'start_col':  'ps.dep',
      'end_col':    'pe.dep',
      'joins':      perim_join,
      'is_intra':   '(ps.dep IS NOT NULL AND pe.dep IS NOT NULL AND ps.dep = pe.dep)',
      'where_geo':  '(ps.dep IS NOT NULL OR pe.dep IS NOT NULL)'
    },
    'reg': {
      'start_col':  'ps.reg',
      'end_col':    'pe.reg',
      'joins':      perim_join,
      'is_intra':   '(ps.reg IS NOT NULL AND pe.reg IS NOT NULL AND ps.reg = pe.reg)',
      'where_geo':  '(ps.reg IS NOT NULL OR pe.reg IS NOT NULL)'
    },
    'epci': {
      'start_col':  'ps.epci',
      'end_col':    'pe.epci',
      'joins':      perim_join,
      'is_intra':   '(ps.epci IS NOT NULL AND pe.epci IS NOT NULL AND ps.epci = pe.epci)',
      'where_geo':  '(ps.epci IS NOT NULL OR pe.epci IS NOT NULL)'
    },
    'country': {
      'start_col':  'ps.country',
      'end_col':    'pe.country',
      'joins':      perim_join,
      'is_intra':   '(ps.country IS NOT NULL AND pe.country IS NOT NULL AND ps.country = pe.country)',
      'where_geo':  '(ps.country IS NOT NULL OR pe.country IS NOT NULL)'
    },
    'aom': {
      'start_col':  'ps.aom',
      'end_col':    'pe.aom',
      'joins':      perim_join,
      'is_intra':   '(ps.aom IS NOT NULL AND pe.aom IS NOT NULL AND ps.aom = pe.aom)',
      'where_geo':  "((ps.aom IS NOT NULL OR ps.aom NOT IN (SELECT aom FROM " ~ aom_region_ref ~ ")) OR (pe.aom IS NOT NULL OR pe.aom NOT IN (SELECT aom FROM " ~ aom_region_ref ~ ")))"
    },
    'plm': {
      'start_col':  'COALESCE(plm_s.com, c.start_geo_code)',
      'end_col':    'COALESCE(plm_e.com, c.end_geo_code)',
      'joins':      'LEFT JOIN plm_perim plm_s ON plm_s.arr = c.start_geo_code LEFT JOIN plm_perim plm_e ON plm_e.arr = c.end_geo_code',
      'is_intra':   '(COALESCE(plm_s.com, c.start_geo_code) = COALESCE(plm_e.com, c.end_geo_code))',
      'where_geo':  '(plm_s.com IS NOT NULL OR plm_e.com IS NOT NULL)'
    },
    'aomreg': {
      'start_col':  'aomr_s.aom',
      'end_col':    'aomr_e.aom',
      'joins':      perim_join ~ " LEFT JOIN " ~ aom_region_ref ~ " aomr_s ON aomr_s.reg = ps.reg LEFT JOIN " ~ aom_region_ref ~ " aomr_e ON aomr_e.reg = pe.reg",
      'is_intra':   '(ps.reg IS NOT NULL AND pe.reg IS NOT NULL AND ps.reg = pe.reg)',
      'where_geo':  '((aomr_s.aom IS NOT NULL OR aomr_e.aom IS NOT NULL) AND NOT (ps.aom IS NOT NULL AND pe.aom IS NOT NULL AND ps.aom = pe.aom))'
    }
  } %}
 
  {% if perim not in perimeters %}
    {{ exceptions.raise_compiler_error("Invalid perimeter: " ~ perim ~ ". Expected: arr, h3z9, h3z8, dep, reg, epci, country, aom, plm, aomreg") }}
  {% endif %}
 
  {%- set cfg = perimeters[perim] -%}
 
{% if perim == 'plm' %}
WITH plm_perim AS (
  SELECT DISTINCT arr, com
  FROM {{ ref('perimeters') }}
  WHERE com IS NOT NULL AND arr <> com
  ORDER BY arr ASC
)
SELECT
{% else %}
SELECT
{% endif %}
  c._id,
  c.operator_id,
  c.operator_trip_id,
  c.operator_class,
  c.driver_key,
  c.passenger_key,
  c.start_datetime_tz AS carpool_datetime,
  c.hour,
  c.driver_revenue,
  c.passenger_seats,
  c.passenger_over_18,
  c.passenger_contribution,
  c.distance,
  c.dist_class,
  {{ cfg.start_col }} AS start_code,
  {{ cfg.end_col }} AS end_code,
  -- Nouveaux utilisateurs
  (d.first_date_driver = c.start_datetime_tz::date) AS is_new_driver,
  (pu.first_date_passenger = c.start_datetime_tz::date) AS is_new_passenger,
  -- incitations operateurs
  c.oi_amount_collectivite,
  c.oi_amount_operator,
  c.oi_amount_other,
  c.oi_amount_total,
  c.oi_collectivite,
  c.oi_operator,
  c.oi_other,
  c.with_incentive,
  -- RPC campaigns
  c.campaigns,
  c.campaigns_amount_total,
  c.campaigns_result_total,
  {{ cfg.is_intra }} AS is_intra
FROM {{ ref('carpools') }} c
LEFT JOIN {{ ref('users') }} d ON d.user_id = c.driver_key
LEFT JOIN {{ ref('users') }} pu ON pu.user_id = c.passenger_key
{{ cfg.joins }}
WHERE {{ time_filter(column, model_column, type, default_start, lookback_nb, lookback_unit) }}
  AND c.valid_acquisition_status = true
  AND {{ cfg.where_geo }}
 
{% endmacro %}
