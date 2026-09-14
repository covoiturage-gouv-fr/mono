{{ config(severity='error', tags=['trusted', 'geo', 'custom_territories']) }}

-- Une commune ne doit se résoudre qu'à UN seul arr par composite (sinon double comptage
-- du recouvrement). Le script de résolution dédoublonne : ce test verrouille l'invariant.
SELECT
  id,
  arr,
  COUNT(*) AS n
FROM {{ ref('custom_territories') }}
GROUP BY id, arr
HAVING COUNT(*) > 1
