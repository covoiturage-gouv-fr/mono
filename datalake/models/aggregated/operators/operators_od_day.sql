{#
  Index composite = clé du delete+insert. Sans lui, le DELETE incrémental (semi-jointure
  sur les 4 colonnes) balaie toute la table (~12 M lignes) => très lent. operator_id seul
  est peu sélectif ; le composite le remplace et couvre les lookups par operator_id (préfixe).
#}
{{
  config(
    materialized='incremental',
    incremental_strategy='delete+insert',
    unique_key=['operator_id', 'incremental_date', 'start_code', 'end_code'],
    indexes=[
      { 'columns': ['operator_id', 'incremental_date', 'start_code', 'end_code'] },
      { 'columns': ['incremental_date'] },
      { 'columns': ['start_code', 'end_code'] }
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
