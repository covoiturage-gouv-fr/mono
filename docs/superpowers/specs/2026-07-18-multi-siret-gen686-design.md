# GEN-686 — Accès multi-SIRET / multi-périmètre — Design

- **Ticket** : GEN-686 « Accès Multi-SIRET ATMB » (slice concret) ; GEN-473 « multi-compte territoire » (vision cible).
- **Date** : 2026-07-18 · **Révision** : v2 (post-revues architecte BDD / sécurité / UX)
- **Statut** : design validé, prêt pour le plan d'implémentation.
- **Ambition** : concevoir le **modèle cible**, le **livrer incrémentalement** en démarrant par ATMB.

> **Changelog v2** — Intègre les corrections des 3 revues expertes : durcissement sécurité (revalidation DB de la bascule, gate des champs privilégiés + hiérarchie de rôle, fail-closed `login_siren`, purge de session à la révocation, `regenerate` anti-fixation), backfill défensif + dual-write expand/contract (BDD), et refonte du flow d'édition (UX). Voir §12 pour la trace des findings.

## 1. Problème & recadrage

Besoin : un utilisateur doit avoir un **SIRET de connexion** (auth ProConnect) **découplé** des **périmètres** (opérateurs / territoires) sur lesquels il agit (campagnes, exports).

Recadrage clé (exploration code) : **dans RPC, le SIRET n'est pas la clé de périmètre.** L'accès est scopé par `operator_id` **XOR** `territory_id` (→ codes géo). Le SIRET est *dérivé* au login et sert de **gate ProConnect** uniquement. « Multi-SIRET » = deux besoins séparables :

1. **Connexion** — découpler le credential ProConnect du périmètre (gate SIREN plus 1:1).
2. **Périmètre** — un compte porte plusieurs territoires et bascule entre eux (contexte actif).

## 2. Décisions de cadrage

| # | Décision | Choix |
|---|----------|-------|
| Ambition | ciblé / cible incrémental / big-bang | **Cible, incrémental** |
| Exposition | contexte actif / agrégé | **Contexte actif** — un seul scope en session à la fois |
| Gouvernance | admin RPC / délégation / self-service | **Octroi `registry.admin` uniquement (V1)** |
| Modèle | chaînes SIRET / références d'entité | **Références d'entité** ; SIRET = gate connexion seulement |
| Homogénéité | mixte / XOR | **XOR homogène** ; `role` unique sur `auth.users` ; **contrainte DB** (v2) |
| Cardinalité | — | **Multi-territoires OUI ; multi-opérateurs NON** (opérateur 1:1) |
| Modèle de contexte | session serveur durcie / URL par onglet | **Session serveur enforced + durcie** (v2, cf. §6) |

### Règle métier centrale

- User **territoire** → 1..N périmètres. User **opérateur** → **exactement 1** (1:1).
- « Multi-SIRET connexion opérateur » (ATMB) = **plusieurs users → même opérateur**, chacun son `login_siren` (GEN-473 Cas#4), **pas** 1 user → plusieurs opérateurs.

## 3. État du code (points d'ancrage)

- **User** : `auth.users` — scopé `operator_id` **XOR** `territory_id`, **sans FK ni CHECK** (integers nus). Pas de colonne SIRET. Dérivé au login dans `auth/providers/UserRepository.ts` (`authenticateByEmail`).
- **Gate ProConnect** : `auth/providers/ProConnectOIDCProvider.ts:140-153` → `failsSirenCheck()` (fail-closed *par accident* via `String(null)`) ; `registry.admin` bypass.
- **Seam session→contexte** : `proxy/HttpTransport.ts:377-389` injecte `session.user` en `call.user` par requête ; `getTerritoryInfos()` (`:482-505`) recalcule `authorizedZoneCodes` — **résolution géo, PAS revalidation d'appartenance**.
- **Auth de session** : `authGuard.ts` = **no-op sur le chemin cookie** (n'agit que si header `authorization`). `/auth/me` (`AuthRouter.ts:95`) rejette 401 lui-même si pas de session. Callback `AuthRouter.ts:48-52` pose `session.user` **sans `regenerate()`**.
- **CRUD user** : `dashboard/providers/UsersRepository.ts:100-165` — écrit `role`/`operator_id`/`territory_id` **sans contrôle de rôle cible ni de champ** ; gaté seulement scope-local (`HasPermissionByScopeMiddleware` teste `params.territory_id === call.user.territory_id`).
- **Précédents réutilisables** : pattern `authorizedZoneCodes`, `territory_group_selector` (pivot many-per-entity), scoping en tableau (`ExportParams.operator_id: number[]`, `= ANY(...)`).

### Blast radius de `auth.users.{operator_id,territory_id}`

- **Cat A — lectures colonne SQL** (à migrer) : seam login (`auth/providers/UserRepository.ts`) + `dashboard/providers/UsersRepository.ts` (filtres list/delete, projection, LEFT JOIN recherche).
- **Cat B — lectures objet session `call.user.*`** (SÛRES, suivent la bascule) : **tout l'aval** (export/policy/territoire/opérateur/credentials + tous les middlewares de scope). Aucun changement.
- **Cat C — écritures** : `dashboard/providers/UsersRepository.ts` create/update.
- **Non concerné** : `accessTokenMiddleware.ts` (chemin machine Dex) — `operator_id` du token, pas de `auth.users`.

**Migration code réelle = 2 fichiers** : seam login + `UsersRepository` dashboard.

## 4. Modèle de données cible

```sql
CREATE EXTENSION IF NOT EXISTS btree_gist;   -- pour la contrainte d'homogénéité

CREATE TABLE auth.user_scopes (
  _id          serial PRIMARY KEY,
  user_id      int  NOT NULL REFERENCES auth.users(_id) ON DELETE CASCADE,
  operator_id  int  NULL REFERENCES operator.operators(_id),
  territory_id int  NULL REFERENCES territory.territory_group(_id),
  is_default   boolean NOT NULL DEFAULT false,
  created_at   timestamptz NOT NULL DEFAULT now(),
  scope_type   text GENERATED ALWAYS AS
                 (CASE WHEN operator_id IS NOT NULL THEN 'o' ELSE 't' END) STORED,
  CHECK (num_nonnulls(operator_id, territory_id) = 1)                 -- XOR par ligne
);

-- unicité / cardinalité métier
CREATE UNIQUE INDEX user_scopes_territory_key       ON auth.user_scopes(user_id, territory_id) WHERE territory_id IS NOT NULL;
CREATE UNIQUE INDEX user_scopes_single_operator_key ON auth.user_scopes(user_id)               WHERE operator_id  IS NOT NULL; -- opérateur 1:1 (subsume le dédoublonnage op)
CREATE UNIQUE INDEX user_scopes_one_default_key     ON auth.user_scopes(user_id)               WHERE is_default;             -- ≤ 1 défaut / user

-- homogénéité XOR par user (v2, non-optionnelle) : interdit op + territoire sur un même user
ALTER TABLE auth.user_scopes
  ADD CONSTRAINT user_scopes_homogeneous_excl EXCLUDE USING gist (user_id WITH =, scope_type WITH <>);

-- lookup inverse (liste dashboard filtrée par operator_id / territory_id)
CREATE INDEX user_scopes_operator_idx  ON auth.user_scopes(operator_id)  WHERE operator_id  IS NOT NULL;
CREATE INDEX user_scopes_territory_idx ON auth.user_scopes(territory_id) WHERE territory_id IS NOT NULL;
```

- `auth.user_scopes` = **source de vérité unique** des périmètres autorisés (défaut inclus via `is_default`).
- `auth.users.{operator_id, territory_id}` → **dépréciées** : conservées + **dual-writées** pendant la transition (cf. §8), droppées en PR de suivi.
- `auth.users.login_siren varchar(9)` — **gate ProConnect uniquement**, découplé des périmètres.
- **Index redondant retiré** (`user_scopes_single_operator_key` subsume le couple op). **Nommage `*_key`/`*_idx`** conforme au schéma existant (pas de `ux_`).
- `ON DELETE CASCADE` sur `user_id` ; **NO ACTION** sur operator/territory → supprimer un territoire référencé est **bloqué** (RESTRICT) : nouveau couplage, les flux de hard-delete territoire/opérateur devront purger `user_scopes` d'abord.
- **Territoires custom** (GEN-473) : `territory_group` sans company/SIRET, pointé via `territory_id` comme un territoire classique.

## 5. Auth & ProConnect (le seam)

- `failsSirenCheck` : compare **SIREN(ProConnect) vs `auth.users.login_siren`**. `registry.admin` bypass.
  **Fail-closed explicite (v2)** : pour tout rôle **non-admin**, `login_siren` NULL/vide ⇒ **échec** du gate (jamais bypass). Ne pas dépendre de `String(null)`. Test dédié.
- `authenticateByEmail` réécrit : (1) match email ; (2) renvoie `login_siren` (gate) ; (3) charge le scope **par défaut** (`user_scopes WHERE is_default`) → seed `session.user.operator_id/territory_id` + libellé/SIRET d'affichage. **Fallback (v2)** : si aucun `is_default`, prendre `MIN(_id)` (login jamais cassé) ; (4) charge la **liste** des scopes → `session.user.scopes[]` (dropdown).
- `UserInterface` (session) gagne `scopes: Array<{ operator_id?, territory_id?, label, siret? }>` ; garde `operator_id/territory_id` = scope actif.
- **Anti-fixation (v2)** : au callback, `req.session.regenerate()` **avant** d'attacher `user`, puis `save()`.
- Chemin machine (Dex) inchangé.

## 6. Bascule de contexte (users territoire) — durcie

**Route `POST /auth/context` `{ territory_id }`** dans `AuthRouter` :
1. **401 si `!req.session?.user`** (ne pas se reposer sur `authGuard`, no-op sur cookie).
2. **Refuser** si l'user est de type opérateur (règle §2 : seul le type territoire bascule).
3. **Revalidation DB (v2, C1/E1)** : `SELECT 1 FROM auth.user_scopes WHERE user_id = <session user> AND territory_id = <body.territory_id>` — **matcher strictement sur le champ `territory_id`** (jamais « le nombre est dans mes scopes » → IDOR par collision op/territoire). 403 si absent. La liste `session.scopes[]` ne sert **qu'à peindre l'UI**, jamais à autoriser.
4. **Réécrire tout le tuple (v2, M3)** : `session.user.territory_id = territory_id` **et** `session.user.operator_id = null` ; `req.session.save()`.
5. Réponse : le **contexte actif** (id + libellé) — renvoyé aussi dans les réponses d'actions conséquentes pour réconciliation front.

**Sécurité du modèle de session (v2)** — on garde la **session serveur comme vérité enforced** (Catégorie B gratuite) mais on la durcit :
- **Revalidation DB à chaque switch** (ci-dessus) — la bascule ne fait jamais confiance au cache session.
- **Purge des sessions à la révocation** (C1/F1) : au retrait d'un scope ou changement de rôle d'un user, invalider ses sessions Redis (index sessions par `user_id`). Sinon un scope retiré reste accessible tant que le cookie vit sur le scope *actif* courant.
- **`maxAge` de session raccourci** pour borner l'exposition résiduelle.
- **Attention `authorizedZoneCodes`** : `getTerritoryInfos` recalcule la **géo**, pas le **droit** — ce n'est pas une revalidation d'appartenance. La revalidation vient du switch (DB) + de la purge à la révocation.

**Défaut & reload** : login frais → `is_default`. Session Redis conserve le scope basculé (survit au reload). Miroir client (sessionStorage) = **affichage** ; sur `focus`/`visibilitychange`, re-GET du contexte serveur + reconcilie. **Ne jamais** traiter le miroir client comme source de vérité (confused-deputy multi-onglets — le serveur enforce, mais l'UI ne doit pas mentir).

## 7. Gestion des utilisateurs (dashboard) — gate au niveau action

Fichier central : `dashboard/providers/UsersRepository.ts` + actions.

**Autorisation (v2, C2/E4) — le point le plus sensible :**
- Les **champs/opérations privilégiés** (`login_siren`, octroi/retrait de scope multi-territoire) passent par des **actions dédiées gatées par une permission détenue seulement par `registry.admin`** (ex. `registry.user.manageScopes`), vérifiée **au niveau action** (pas l'écran). Un middleware rejette la présence de ces champs si le caller n'a pas la permission.
- **Garde de hiérarchie de rôle (v2)** : createUser/updateUser refusent d'attribuer un `role` de rang ≥ celui du caller (un `territory.admin` **ne peut pas** créer/promouvoir un `registry.*`). Corrige un **trou pré-existant** que le découplage `login_siren` armerait.

**CRUD :**
- **Lister les users d'un groupe** : `JOIN auth.user_scopes us … WHERE us.operator_id = X / us.territory_id = ANY(<scopes admin>)`, avec **`DISTINCT`/`GROUP BY users._id`** (un user multi-territoires apparaît sinon N fois).
- **Créer** : INSERT `auth.users` (**+ dual-write `operator_id/territory_id` pendant l'expand**, cf. §8) + INSERT `user_scopes` (`is_default=true`) + `login_siren` saisi par un `registry.admin`.
- **Gérer les scopes** : ajout/retrait de territoires. **Retrait du défaut ⇒ promotion auto** d'un autre scope (v2, I2) ; **interdire** de vider tous les scopes (user orphelin = login cassé). Ajout d'un 2ᵉ opérateur → refus métier propre (avant la contrainte DB).
- **Supprimer** : scoping via jointure pivot. `ON DELETE CASCADE` nettoie `user_scopes` ; **purger aussi les sessions Redis** (cf. §6).

**UX (v2, revue UX) :**
- **Bascule (header)** : **combobox recherchable** (pas `<select>` natif — dizaines de territoires possibles), affiche le **nom** (corriger `ProfilButton` qui montre l'ID brut) + coche l'actif. 1 seul scope → **badge statique**, pas de menu. **Rappel du périmètre actif au point d'action** (modale de confirmation d'export/campagne), pas seulement le header. Feedback au switch : invalidation données + toast `success` + `aria-live` (RGAA) + confirmation si saisie en cours.
- **Formulaire user** : sortir de la modale saturée → **page dédiée** ou **`fieldset` DSFR** segmentés (Identité / Connexion / Périmètres). CRUD scopes = **`Table` + radio "défaut" + `Autocomplete` d'ajout**.
- **`login_siren`** : `label="SIREN de connexion (ProConnect)"`, `hint="9 chiffres — distinct du SIRET du territoire"`, validation `/^\d{9}$/`, `inputMode=numeric`, **pré-remplissage** suggéré `LEFT(siret_territoire, 9)` modifiable.
- **Mono-opérateur** : pour un user opérateur, **pas d'affordance d'ajout** (un seul `Select`, 1:1).
- **Gating** : un `territory.admin` → section Périmètres + `login_siren` **masquées** (pas grisées).
- **États limites** : user sans territoire (`registry.admin` → libellé « Administration RPC »), retrait du défaut, bascule vers scope révoqué → 403 + toast + refresh liste.

## 8. Migration & backfill (expand / contract, défensif)

**Audit préalable OBLIGATOIRE (v2, C3)** — `auth.users` n'a jamais eu FK/CHECK :
```sql
SELECT count(*) FROM auth.users WHERE operator_id IS NOT NULL AND territory_id IS NOT NULL;      -- both-set
SELECT count(*) FROM auth.users u WHERE u.operator_id IS NOT NULL
  AND NOT EXISTS (SELECT 1 FROM operator.operators o WHERE o._id = u.operator_id);               -- orphelins op
SELECT count(*) FROM auth.users u WHERE u.territory_id IS NOT NULL
  AND NOT EXISTS (SELECT 1 FROM territory.territory_group g WHERE g._id = u.territory_id);        -- orphelins territoire
```
Décider explicitement du sort des both-set/orphelins (nettoyer avant, ou exclure sciemment).

**PR « expand »** :
1. `CREATE EXTENSION btree_gist` + `CREATE TABLE auth.user_scopes` + contraintes/index (§4). Index/CHECK **avant** le backfill (échoue vite sur données sales).
2. `ALTER TABLE auth.users ADD COLUMN login_siren varchar(9)`.
3. **Backfill défensif en 2 INSERT idempotents** :
   ```sql
   INSERT INTO auth.user_scopes (user_id, operator_id, is_default)
   SELECT u._id, u.operator_id, true FROM auth.users u
   WHERE u.operator_id IS NOT NULL
     AND NOT EXISTS (SELECT 1 FROM auth.user_scopes s WHERE s.user_id = u._id);

   INSERT INTO auth.user_scopes (user_id, territory_id, is_default)
   SELECT u._id, u.territory_id, true FROM auth.users u
   WHERE u.territory_id IS NOT NULL AND u.operator_id IS NULL
     AND NOT EXISTS (SELECT 1 FROM auth.user_scopes s WHERE s.user_id = u._id);
   ```
4. **Backfill `login_siren`** = `LEFT(COALESCE(o.siret, c.siret), 9)` via **LEFT JOIN** operator / territory_group→company (ne pas dropper les users sans company). `siret` mal formé → surveiller.
5. **Bascule code + dual-write (v2, I3)** : le nouveau code **lit** `user_scopes` mais **continue d'écrire** `auth.users.operator_id/territory_id` (dual-write) tant que le contract n'a pas confirmé qu'aucun pod ne les lit. Évite l'invisibilité inter-pods pendant le rolling update.
6. **Contrôles post-migration BLOQUANTS (gate de déploiement, pas advisory)** : chaque user non-admin a **exactement 1** `is_default` ; `login_siren` peuplé partout où un SIRET existe ; 0 both-set/orphelin résiduel.

**PR « contract »** : retirer le dual-write, puis `DROP COLUMN auth.users.operator_id, territory_id` une fois qu'aucun code ne les lit (grep) et le déploiement stabilisé.

*(Migrations jouées au déploiement k8s ; expand/contract + dual-write évitent tout couplage migration↔rollback et l'invisibilité inter-pods.)*

## 9. Tests (TDD)

- **Unit** :
  - `failsSirenCheck` : match / mismatch / bypass admin / **`login_siren` NULL ∧ non-admin ⇒ échec** (fail-closed).
  - Contraintes pivot : refus 2ᵉ opérateur, multi-territoires OK, unicité `is_default`, **homogénéité (op+territoire refusé)**.
  - `/auth/context` : 403 hors liste, **403 sur collision op/territoire** (`operator_id:42` ⇏ `territory_id:42`), **401 sans session**, refus si user opérateur, mutation tuple complet.
- **Intégration (DB)** :
  - `UsersRepository` list/create/delete via jointure pivot (+ `DISTINCT` multi-territoires) ; `authenticateByEmail` seed défaut + fallback `MIN(_id)` + `scopes[]` + `login_siren`.
  - **Escalade** : `territory.admin` → **403** sur saisie `login_siren` / octroi de scope / attribution `role: registry.*`.
  - **Révocation** : retrait de scope → sessions Redis de l'user purgées → accès coupé.
- **e2e (stack)** : login → `/auth/context` (switch) → export scopé au **nouveau** territoire + résolution territoire correcte → ferme les TODO ticket « filtrage exports » et « résolution territoire ».
- **Backfill** : correction/idempotence sur données seed (dont both-set/orphelins).

## 10. Évaluation risque & difficulté (v2)

| Volet | Difficulté | Risque | Note |
|-------|-----------|--------|------|
| Modèle `user_scopes` + contraintes | Faible | Faible | Contraintes DB portent le métier + homogénéité |
| Seam login + `failsSirenCheck` fail-closed | Moyenne | Moyen | Chemin critique ; tests fail-closed obligatoires |
| Bascule `/auth/context` durcie | Moyenne | Moyen | Revalidation DB + typage strict + 401/refus opérateur |
| **Durcissement auth (C1/C2)** | Moyenne | **Élevé si omis** | Purge session, gate champ privilégié, hiérarchie de rôle — **bloquants sécurité** |
| Aval (exports, campagnes, scoping) | Nulle | Faible | Catégorie B suit la session |
| CRUD dashboard + UX | Moyenne-Haute | Moyen | Page/fieldset, table scopes, gating action |
| Migration/backfill défensif | Moyenne | Moyen | Audit + 2 INSERT idempotents + dual-write + contrôles bloquants |
| Front (combobox, rappel périmètre) | Moyenne | Faible | Réutilise `Autocomplete`/patterns DSFR |

**Risques résiduels** : (1) révocation vs sessions actives → mitigé par purge Redis ; (2) escalade `registry.admin` → mitigé par gate action + hiérarchie de rôle ; (3) cohérence backfill → mitigé par audit + contrôles bloquants.

## 11. Hors-scope (évolutions futures)

- Périmètre **agrégé/simultané** (export « tous mes territoires ») — jointures `territory_id` en tableau.
- **Comptes hétérogènes** (opérateur + territoire) avec rôle par membership.
- **Délégation inter-orgs** + **self-service ProConnect** (alias SIRET même domaine).
- **Multi-SIRET de connexion pour un même user** (alias ProConnect sur un email) — V1 = un `login_siren`/user.
- Modèle **URL-par-onglet** pour le contexte (retenu : session serveur durcie en V1).
- Convergence avec le mode `simulate` admin existant (deux « je change de contexte » à unifier).

## 12. Trace des revues expertes (2026-07-18)

- **BDD** : backfill défensif (both-set/orphelins, 2 INSERT idempotents), index redondant retiré, dual-write expand, index lookup inverse, `DISTINCT`, exclusion constraint homogénéité, nommage `*_idx/*_key`, promotion défaut au retrait.
- **Sécurité (OWASP)** : **C1** revalidation DB de la bascule + purge session à la révocation (le recalcul par-requête ne revérifie pas l'appartenance) ; **C2** gate des champs privilégiés + hiérarchie de rôle (sinon élévation `registry.admin` via createUser) ; `/auth/context` typé + 401 anonyme + refus opérateur ; fail-closed `login_siren` NULL ; `regenerate` anti-fixation ; réécriture tuple complet à la bascule.
- **UX (DSFR)** : combobox recherchable + noms, rappel périmètre au point d'action, page/`fieldset`, table+radio+Autocomplete pour les scopes, `login_siren` étiqueté/validé/pré-rempli, mono-opérateur sans affordance, gating = masquer, états limites.
