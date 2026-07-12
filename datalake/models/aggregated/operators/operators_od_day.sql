{#
  Composite réordonné, (start_code, end_code) en tête = clé du delete+insert. Sinon le
  planificateur choisit l'index ['start_code', 'end_code'] seul, peu sélectif (une paire
  origine-destination revient sur des milliers de lignes) : le DELETE incrémental balaie
  cet index et devient catastrophique (> 1 h par fenêtre). Avec (start_code, end_code) en
  tête, le composite sert le DELETE (sonde par ligne sur les 4 colonnes) ET les lookups par
  (start_code, end_code) via son préfixe gauche ; l'index ['start_code', 'end_code'] seul
  devient redondant et est retiré.
#}
{{
  config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key=['operator_id', 'incremental_date', 'start_code', 'end_code'],
    indexes=[
      { 'columns': ['start_code', 'end_code', 'operator_id', 'incremental_date'] },
      { 'columns': ['incremental_date'] }
    ],
    tags=['aggregated', 'operators', 'od', 'daily']
  )
}}
WITH filtered_carpools AS (
  {{ filtered_carpools('arr', lookback_nb=3, lookback_unit='day') }}
)


SELECT
  operator_id,
  operator_name,
  carpool_datetime::date AS incremental_date,
  start_code,
  end_code,
  {{ od_agg_columns() }}
FROM filtered_carpools
WHERE operator_id IS NOT NULL
GROUP BY 1, 2, 3, 4, 5
