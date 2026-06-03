-- =============================================================================
-- BACKFILL : rétablir acquisition_status = 'terms_violation_error'
--            sur les trajets dont le statut a été écrasé par le batch géo.
--
-- Contexte  : voir docs/fixes/terms-violation-status-clobber.md
-- Cible     : carpool_v2.status
-- Volume    : ~213 376 lignes au 2026-06-03
-- Idempotent: oui (les lignes déjà en terms_violation_error sont exclues)
--
-- /!\ NE PAS câbler comme migration auto (api/src/db/migrations).
--     À exécuter DÉLIBÉRÉMENT, après revue + décision équipe (cf. AVERTISSEMENT).
-- =============================================================================

-- -----------------------------------------------------------------------------
-- AVERTISSEMENT (à trancher AVANT exécution)
-- -----------------------------------------------------------------------------
-- 1. Effets aval : est-ce que le moteur d'incitations / paiements relit
--    acquisition_status ? Des trajets passés à tort en 'processed' ont pu être
--    incités/payés. Repasser en 'terms_violation_error' a posteriori peut créer
--    des incohérences comptables. Confirmer avec l'équipe métier (IDFM, Karos,
--    OuestGo cités dans le thread).
-- 2. Conciliations manuelles : le process manuel « valider + supprimer les
--    labels » sort ces trajets du périmètre (labels vides => exclus). Restent
--    donc les « ratés » (statut forcé à OK mais labels non supprimés), qu'on veut
--    bien repasser en violation. Comportement voulu.
-- 3. Lancer le backfill APRÈS le déploiement du fix code, pour ne pas ré-écraser.
--    (Sans risque immédiat : un trajet déjà géocodé n'est plus repris par
--     findProcessable, mais l'ordre reste la règle de prudence.)
-- -----------------------------------------------------------------------------


-- == 1) DRY RUN : compter le périmètre exact avant de toucher quoi que ce soit ==
SELECT
  s.acquisition_status,
  count(*) AS n
FROM carpool_v2.status s
JOIN carpool_v2.terms_violation_error_labels l ON l.carpool_id = s.carpool_id
WHERE cardinality(l.labels) > 0
  AND s.acquisition_status <> 'terms_violation_error'
GROUP BY s.acquisition_status
ORDER BY n DESC;
-- Attendu (2026-06-03) : processed = 213376, failed = 1


-- == 2) SAUVEGARDE de l'état avant backfill (audit / rollback) ==
-- Garde une trace des carpool_id touchés et de leur ancien statut.
CREATE TABLE IF NOT EXISTS carpool_v2._backfill_tve_20260603 AS
SELECT
  s.carpool_id,
  s.acquisition_status AS old_status,
  now()                AS captured_at
FROM carpool_v2.status s
JOIN carpool_v2.terms_violation_error_labels l ON l.carpool_id = s.carpool_id
WHERE cardinality(l.labels) > 0
  AND s.acquisition_status = 'processed';   -- on ne backfille QUE depuis 'processed'


-- == 3) BACKFILL ==
-- Option A (recommandée) : une seule transaction. ~213k lignes ciblées,
-- verrouille uniquement les lignes concernées, suffisamment rapide.
UPDATE carpool_v2.status s
SET acquisition_status = 'terms_violation_error'
FROM carpool_v2.terms_violation_error_labels l
WHERE l.carpool_id = s.carpool_id
  AND cardinality(l.labels) > 0
  AND s.acquisition_status = 'processed';
-- Note : updated_at se met à jour seul si un trigger l'impose ; sinon ajouter
--        ", updated_at = now()" ci-dessus selon la convention de la table.

-- Option B (prudence / forte concurrence) : par lots de 10 000, à lancer
-- en boucle (psql) jusqu'à 0 ligne affectée. Décommenter si besoin.
-- WITH cte AS (
--   SELECT s.carpool_id
--   FROM carpool_v2.status s
--   JOIN carpool_v2.terms_violation_error_labels l ON l.carpool_id = s.carpool_id
--   WHERE cardinality(l.labels) > 0
--     AND s.acquisition_status = 'processed'
--   LIMIT 10000
-- )
-- UPDATE carpool_v2.status s
-- SET acquisition_status = 'terms_violation_error'
-- FROM cte
-- WHERE s.carpool_id = cte.carpool_id;


-- == 4) VÉRIFICATION post-backfill ==
-- Doit renvoyer 0 (plus aucun trajet 'processed' avec label de violation).
SELECT count(*) AS restant
FROM carpool_v2.status s
JOIN carpool_v2.terms_violation_error_labels l ON l.carpool_id = s.carpool_id
WHERE cardinality(l.labels) > 0
  AND s.acquisition_status = 'processed';


-- == 5) ROLLBACK (si besoin, depuis la sauvegarde) ==
-- UPDATE carpool_v2.status s
-- SET acquisition_status = b.old_status
-- FROM carpool_v2._backfill_tve_20260603 b
-- WHERE s.carpool_id = b.carpool_id
--   AND s.acquisition_status = 'terms_violation_error';
