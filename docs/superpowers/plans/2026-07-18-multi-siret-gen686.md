# Multi-SIRET / multi-périmètre (GEN-686) — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Découpler le SIRET de connexion (gate ProConnect) des périmètres d'un utilisateur, et permettre à un compte territoire de porter plusieurs territoires avec bascule de contexte — via une table pivot `auth.user_scopes`.

**Architecture:** Table pivot `auth.user_scopes` = source de vérité des périmètres autorisés (défaut porté par `is_default`). Le scope actif vit en session serveur (Redis) ; l'aval (Catégorie B) lit `call.user.operator_id/territory_id` inchangé. `login_siren` sur `auth.users` sert uniquement le gate ProConnect. Bascule via `POST /auth/context` revalidée en DB. Migration expand/contract avec dual-write.

**Tech Stack:** Deno 2.x, TypeScript, Express, ILOS (Inversify), PostgreSQL, Redis (express-session), ProConnect OIDC. Front : Next.js 15 / React 19 / DSFR (`app-partners`).

## Global Constraints

- **TDD** : test qui échoue → implémentation minimale → test passe → commit. (CLAUDE.md)
- **Undercover** : aucune mention Claude/Anthropic dans commits/PR/trailers.
- **Dépôt public** : aucun montant / nb de trajets / info entreprise non publique dans code/commits/PR.
- **Jamais de commit sur `main`** : chaque tranche = worktree + PR.
- **Français** : descriptions de PR et libellés en français ; « la spec » (féminin).
- **Nommage SQL** : index `<table>_<cols>_idx`, contraintes uniques `<table>_<cols>_key` (jamais `ux_`). `timestamptz`.
- **Fail-closed** : tout doute sur une autorisation → refus.
- Spec de référence : `docs/superpowers/specs/2026-07-18-multi-siret-gen686-design.md`.

---

## Contrats partagés (FIGÉS — tous les agents s'y conforment)

Ces signatures sont le contrat inter-tranches. Ne pas les renommer sans mettre à jour ce bloc.

```typescript
// Objet user en session (proxy/types/UserInterface.ts) — étendu par T2
interface UserScope {
  operator_id?: number;   // exclusif avec territory_id
  territory_id?: number;
  label: string;          // nom d'affichage (operator.name | territory_group.name)
  siret?: string;         // SIRET du périmètre, affichage seul
}
interface UserInterface {
  // ...existant...
  operator_id?: number;      // = scope ACTIF (inchangé pour la Cat. B)
  territory_id?: number;     // = scope ACTIF
  scopes: UserScope[];       // AJOUT T2 — liste des périmètres autorisés (peindre l'UI)
  // login_siren N'est PAS en session (consommé au gate, avant session)
}

// Permission (auth/config/permissions.ts) — AJOUT T4
// "registry.user.manageScopes" détenue UNIQUEMENT par registry.admin

// Route bascule (auth/AuthRouter.ts) — AJOUT T3
// POST /auth/context  body: { territory_id: number }  → 200 { territory_id, label } | 401 | 403

// Repository pivot (auth/providers/UserScopeRepository.ts) — CRÉÉ T1, consommé T2/T3/T4
class UserScopeRepository {
  // toutes requêtes paramétrées
  findByUser(userId: number): Promise<UserScope[]>;                          // pour scopes[]
  findDefault(userId: number): Promise<{operator_id?: number; territory_id?: number} | null>;
  userHasTerritory(userId: number, territoryId: number): Promise<boolean>;   // revalidation /auth/context
  addTerritory(userId: number, territoryId: number, isDefault?: boolean): Promise<void>;
  removeTerritory(userId: number, territoryId: number): Promise<void>;       // promeut un autre défaut si besoin
  setOperator(userId: number, operatorId: number): Promise<void>;
}
```

**Ordre du merge train (stacking worktrees) :**
```
T1 (base = main)  →  merge en 1er
T2 (base = T1)    ┐ parallèles après T1
T4 (base = T1)    ┘
T3 (base = T2)
T5 (base = T2+T3)
T6 (base = tout mergé + stabilisé)  →  drop colonnes
```

---

## Structure de fichiers

- `api/src/db/migrations/2026071810XXXX-create-user-scopes.sql` — **créé T1** : table + contraintes + backfill + `login_siren`.
- `api/src/pdc/services/auth/providers/UserScopeRepository.ts` — **créé T1** : accès pivot.
- `api/src/pdc/services/auth/providers/UserRepository.ts` — **modifié T2** : `authenticateByEmail` (seed défaut + scopes[] + login_siren + fallback).
- `api/src/pdc/services/auth/providers/ProConnectOIDCProvider.ts` — **modifié T2** : `failsSirenCheck` fail-closed sur `login_siren`.
- `api/src/pdc/services/auth/AuthRouter.ts` — **modifié T2** (`regenerate`) **+ T3** (route `/auth/context`).
- `api/src/pdc/proxy/types/UserInterface.ts` — **modifié T2** : `scopes[]`.
- `api/src/pdc/services/auth/providers/SessionRepository.ts` — **créé T3** : purge des sessions Redis d'un user.
- `api/src/pdc/services/dashboard/providers/UsersRepository.ts` — **modifié T4** : list/create/update/delete via pivot + dual-write.
- `api/src/pdc/services/dashboard/actions/users/*.ts` + `api/src/pdc/services/auth/config/permissions.ts` — **modifié T4** : gate `manageScopes` + hiérarchie de rôle.
- `app-partners/src/components/layout/AppHeader.tsx`, `ProfilButton.tsx`, `AuthProvider.tsx`, `administration/tables/UsersTable.tsx` — **modifié T5**.
- Migration `2026XXXX-drop-user-scope-legacy-columns.sql` — **créé T6**.

---

## Task 1 (T1) : Migration `auth.user_scopes` + backfill défensif + `UserScopeRepository`

**Files:**
- Create: `api/src/db/migrations/2026071810XXXX-create-user-scopes.sql`
- Create: `api/src/pdc/services/auth/providers/UserScopeRepository.ts`
- Test: `api/src/pdc/services/auth/providers/UserScopeRepository.integration.spec.ts`

**Interfaces:**
- Produces: table `auth.user_scopes`, colonne `auth.users.login_siren`, classe `UserScopeRepository` (cf. contrats partagés).

- [ ] **Step 1 : Audit préalable des données sales** (à jouer via MCP `prod_ro` avant d'écrire la migration ; consigner les comptes)

```sql
SELECT count(*) FROM auth.users WHERE operator_id IS NOT NULL AND territory_id IS NOT NULL;   -- both-set
SELECT count(*) FROM auth.users u WHERE u.operator_id IS NOT NULL
  AND NOT EXISTS (SELECT 1 FROM operator.operators o WHERE o._id = u.operator_id);             -- orphelins op
SELECT count(*) FROM auth.users u WHERE u.territory_id IS NOT NULL
  AND NOT EXISTS (SELECT 1 FROM territory.territory_group g WHERE g._id = u.territory_id);      -- orphelins territoire
```
Si comptes > 0 : décider (nettoyer avant / exclure). Le backfill idempotent ci-dessous exclut déjà les both-set (priorité opérateur) et n'insère pas d'orphelins grâce aux FK actives (échec = donnée à nettoyer d'abord).

- [ ] **Step 2 : Écrire la migration**

```sql
CREATE EXTENSION IF NOT EXISTS btree_gist;

CREATE TABLE auth.user_scopes (
  _id          serial PRIMARY KEY,
  user_id      int  NOT NULL REFERENCES auth.users(_id) ON DELETE CASCADE,
  operator_id  int  NULL REFERENCES operator.operators(_id),
  territory_id int  NULL REFERENCES territory.territory_group(_id),
  is_default   boolean NOT NULL DEFAULT false,
  created_at   timestamptz NOT NULL DEFAULT now(),
  scope_type   text GENERATED ALWAYS AS (CASE WHEN operator_id IS NOT NULL THEN 'o' ELSE 't' END) STORED,
  CHECK (num_nonnulls(operator_id, territory_id) = 1)
);
CREATE UNIQUE INDEX user_scopes_territory_key       ON auth.user_scopes(user_id, territory_id) WHERE territory_id IS NOT NULL;
CREATE UNIQUE INDEX user_scopes_single_operator_key ON auth.user_scopes(user_id)               WHERE operator_id  IS NOT NULL;
CREATE UNIQUE INDEX user_scopes_one_default_key     ON auth.user_scopes(user_id)               WHERE is_default;
ALTER TABLE auth.user_scopes
  ADD CONSTRAINT user_scopes_homogeneous_excl EXCLUDE USING gist (user_id WITH =, scope_type WITH <>);
CREATE INDEX user_scopes_operator_idx  ON auth.user_scopes(operator_id)  WHERE operator_id  IS NOT NULL;
CREATE INDEX user_scopes_territory_idx ON auth.user_scopes(territory_id) WHERE territory_id IS NOT NULL;

ALTER TABLE auth.users ADD COLUMN login_siren varchar(9);

-- backfill idempotent, opérateur prioritaire sur both-set
INSERT INTO auth.user_scopes (user_id, operator_id, is_default)
SELECT u._id, u.operator_id, true FROM auth.users u
WHERE u.operator_id IS NOT NULL
  AND NOT EXISTS (SELECT 1 FROM auth.user_scopes s WHERE s.user_id = u._id);

INSERT INTO auth.user_scopes (user_id, territory_id, is_default)
SELECT u._id, u.territory_id, true FROM auth.users u
WHERE u.territory_id IS NOT NULL AND u.operator_id IS NULL
  AND NOT EXISTS (SELECT 1 FROM auth.user_scopes s WHERE s.user_id = u._id);

UPDATE auth.users u SET login_siren = LEFT(COALESCE(o.siret, c.siret), 9)
FROM (SELECT _id FROM auth.users) me
LEFT JOIN operator.operators o ON o._id = (SELECT operator_id FROM auth.users WHERE _id = me._id)
LEFT JOIN territory.territory_group g ON g._id = (SELECT territory_id FROM auth.users WHERE _id = me._id)
LEFT JOIN company.companies c ON c._id = g.company_id
WHERE u._id = me._id;
```

- [ ] **Step 3 : Contrôles post-migration bloquants** (dans la migration ou script de vérif)

```sql
-- chaque user avec scope a exactement 1 défaut
SELECT count(*) FROM (SELECT user_id, count(*) FILTER (WHERE is_default) d FROM auth.user_scopes GROUP BY user_id) x WHERE d <> 1;  -- attendu 0
```

- [ ] **Step 4 : Test d'intégration `UserScopeRepository`** (DB via stack — cf. skill `tests-e2e`)

```typescript
// findDefault renvoie le scope is_default ; userHasTerritory=false pour un territoire non attribué
it("findDefault returns the default scope", async () => {
  const uid = await seedUserWithTerritory(200); // helper local
  expect(await repo.findDefault(uid)).toEqual({ territory_id: 200 });
});
it("userHasTerritory is false for a non-granted territory", async () => {
  const uid = await seedUserWithTerritory(200);
  expect(await repo.userHasTerritory(uid, 999)).toBe(false);
});
```

- [ ] **Step 5 : Run** `just dc_e2e` (ou recette d'intégration) — Expected: FAIL (repo absent).
- [ ] **Step 6 : Implémenter `UserScopeRepository`** (requêtes paramétrées, pattern `PgRepositoryProvider` existant). `removeTerritory` : si la ligne retirée était `is_default`, promouvoir `MIN(_id)` restant dans la même transaction ; refuser si c'était le dernier scope.
- [ ] **Step 7 : Run** l'intégration — Expected: PASS.
- [ ] **Step 8 : Commit** `feat(auth): table user_scopes + backfill + login_siren (GEN-686)`

---

## Task 2 (T2) : Auth — seam login, `failsSirenCheck` fail-closed, `scopes[]`, regenerate

**Files:**
- Modify: `api/src/pdc/services/auth/providers/UserRepository.ts` (`authenticateByEmail`)
- Modify: `api/src/pdc/services/auth/providers/ProConnectOIDCProvider.ts` (`failsSirenCheck`)
- Modify: `api/src/pdc/proxy/types/UserInterface.ts` (+`scopes`)
- Modify: `api/src/pdc/services/auth/AuthRouter.ts` (`session.regenerate` au callback)
- Test: `*.spec.ts` associés

**Interfaces:**
- Consumes: `UserScopeRepository` (T1).
- Produces: `session.user.scopes[]`, `session.user.{operator_id,territory_id}` = défaut ; `login_siren` renvoyé au gate.

- [ ] **Step 1 : Test fail-closed** `failsSirenCheck`

```typescript
it("fails closed when login_siren is null and user is not registry.admin", () => {
  expect(failsSirenCheck({ siren: "123456789" }, { login_siren: null, role: "territory.admin" })).toBe(true);
});
it("passes for registry.admin regardless of siren", () => {
  expect(failsSirenCheck({ siren: "999999999" }, { login_siren: null, role: "registry.admin" })).toBe(false);
});
it("passes when proconnect siren matches login_siren", () => {
  expect(failsSirenCheck({ siren: "123456789" }, { login_siren: "123456789", role: "territory.admin" })).toBe(false);
});
```

- [ ] **Step 2 : Run** — FAIL.
- [ ] **Step 3 : Implémenter** `failsSirenCheck` : `registry.admin` → `false` ; sinon `!login_siren` → `true` (fail-closed explicite, pas de `String(null)`) ; sinon comparer `proconnect.siren === login_siren`.
- [ ] **Step 4 : Run** — PASS.
- [ ] **Step 5 : Test `authenticateByEmail`** (intégration) : renvoie `login_siren`, seed `operator_id/territory_id` depuis `is_default`, `scopes[]` peuplé, fallback `MIN(_id)` si aucun `is_default`.
- [ ] **Step 6 : Run** — FAIL.
- [ ] **Step 7 : Implémenter** : match email inchangé ; charger défaut via `UserScopeRepository.findDefault` (fallback `MIN(_id)`) ; résoudre label/siret d'affichage ; charger `scopes[]` via `findByUser` ; renvoyer `login_siren`. Étendre `UserInterface` avec `scopes`.
- [ ] **Step 8 : Anti-fixation** — au callback (`AuthRouter.ts:48-52`), `await new Promise(r => req.session.regenerate(r))` **avant** `req.session.user = user`, puis `save`.
- [ ] **Step 9 : Run** tous les tests auth — PASS.
- [ ] **Step 10 : Commit** `feat(auth): seam login multi-scope + gate fail-closed + regenerate (GEN-686)`

---

## Task 3 (T3) : Bascule `POST /auth/context` durcie + purge de session

**Files:**
- Modify: `api/src/pdc/services/auth/AuthRouter.ts` (route)
- Create: `api/src/pdc/services/auth/providers/SessionRepository.ts` (purge Redis par user)
- Test: `AuthRouter.integration.spec.ts`

**Interfaces:**
- Consumes: `UserScopeRepository.userHasTerritory` (T1), `session.user.scopes` (T2).
- Produces: route `/auth/context` ; `SessionRepository.destroyByUser(userId)` (consommé T4).

- [ ] **Step 1 : Tests route** (intégration, via supertest sur l'app)

```typescript
it("401 when no session user", async () => (await ctx.post("/auth/context", { territory_id: 1 })).status === 401);
it("403 when territory not granted in DB", async () => { /* user a territoire 200, demande 999 */ });
it("403 on op/territory id collision", async () => { /* scope operator_id:42, body territory_id:42 → 403 */ });
it("200 and mutates active tuple (operator_id=null) when granted", async () => { /* territoire 200 accordé */ });
it("refuses switch for operator-type user", async () => { /* 403/400 */ });
```

- [ ] **Step 2 : Run** — FAIL.
- [ ] **Step 3 : Implémenter la route** : (1) `if (!req.session?.user) return 401` ; (2) refuser si user opérateur ; (3) **revalider en DB** `userHasTerritory(user._id, body.territory_id)` (match strict champ) sinon 403 ; (4) `req.session.user.territory_id = body.territory_id ; req.session.user.operator_id = null ; await save` ; (5) répondre `{ territory_id, label }`.
- [ ] **Step 4 : Run** — PASS.
- [ ] **Step 5 : Test `SessionRepository.destroyByUser`** : après purge, la session de l'user n'existe plus dans Redis.
- [ ] **Step 6 : Implémenter** `SessionRepository` : indexer/scanner les sessions par `user_id` (préfixe connect-redis) et `destroy`. Voir `proxy/middlewares/sessionMiddleware.ts` pour le store.
- [ ] **Step 7 : Run** — PASS.
- [ ] **Step 8 : Commit** `feat(auth): route /auth/context revalidée DB + purge session (GEN-686)`

---

## Task 4 (T4) : CRUD dashboard via pivot + gate `manageScopes` + hiérarchie de rôle

**Files:**
- Modify: `api/src/pdc/services/dashboard/providers/UsersRepository.ts` (list/create/update/delete via pivot + **dual-write** colonnes dépréciées)
- Modify: `api/src/pdc/services/auth/config/permissions.ts` (+`registry.user.manageScopes`)
- Modify: `api/src/pdc/services/dashboard/actions/users/{Create,Update}UserAction.ts` + middleware
- Test: `*.spec.ts` + intégration

**Interfaces:**
- Consumes: `UserScopeRepository` (T1), `SessionRepository.destroyByUser` (T3).
- Produces: actions gardées ; liste dashboard via pivot.

- [ ] **Step 1 : Test hiérarchie de rôle** : `territory.admin` créant `role: registry.admin` → **403**.
- [ ] **Step 2 : Test gate champ privilégié** : `territory.admin` fournissant `login_siren`/octroi de scope → **403** ; `registry.admin` → OK.
- [ ] **Step 3 : Test liste via pivot** : user multi-territoires visible des admins concernés, **une seule fois** (`DISTINCT`).
- [ ] **Step 4 : Run** — FAIL.
- [ ] **Step 5 : Implémenter** : permission `registry.user.manageScopes` (matrice `permissions.ts`, détenue par `registry.admin` seul) ; middleware rejetant `login_siren`/`scopes` si permission absente ; garde `roleRank(target) < roleRank(caller)` dans Create/Update ; requête liste `JOIN user_scopes … WHERE … = ANY(...) GROUP BY users._id` ; create/update **dual-write** `auth.users.operator_id/territory_id` **et** `user_scopes` ; delete/retrait de scope → `SessionRepository.destroyByUser`.
- [ ] **Step 6 : Run** — PASS.
- [ ] **Step 7 : Commit** `feat(dashboard): CRUD user multi-scope gaté registry.admin (GEN-686)`

---

## Task 5 (T5) : Front — bascule header + formulaire scopes

**Files:**
- Modify: `app-partners/src/components/layout/AppHeader.tsx` (combobox de bascule)
- Modify: `app-partners/src/components/layout/ProfilButton.tsx` (nom au lieu de l'ID)
- Modify: `app-partners/src/providers/AuthProvider.tsx` + `interfaces/auth.ts` (`scopes[]`, contexte actif)
- Modify: `app-partners/src/app/administration/tables/UsersTable.tsx` (fieldset + table scopes + `login_siren`)
- Test: tests composants existants (pattern du repo)

**Interfaces:**
- Consumes: `scopes[]` (`/auth/me`), route `POST /auth/context` (T3), actions CRUD (T4).

- [ ] **Step 1 : Bascule header** : combobox recherchable (`Autocomplete` façon `SelectGeo.tsx`), affiche `scope.label`, coche l'actif ; 1 seul scope → badge statique. Appelle `/auth/context`, invalide les données (React Query), toast `success` + `aria-live`, confirmation si saisie en cours. Reconcilie sur `focus`/`visibilitychange`.
- [ ] **Step 2 : Rappel du périmètre au point d'action** : `Callout`/`Badge` « Périmètre actif : {label} » en tête des écrans export/campagne + dans la modale de confirmation d'export.
- [ ] **Step 3 : Formulaire user** : segmenter en `fieldset` (Identité / Connexion / Périmètres). `login_siren` : `label="SIREN de connexion (ProConnect)"`, hint « 9 chiffres — distinct du SIRET du territoire », validation `/^\d{9}$/`, pré-remplissage `LEFT(siret,9)`. Périmètres : `Table` + radio défaut + `Autocomplete` d'ajout. User opérateur → un seul `Select`, pas d'ajout. `territory.admin` → sections Connexion/Périmètres **masquées**.
- [ ] **Step 4 : États limites** : 0 scope (`registry.admin` → « Administration RPC »), retrait du défaut (promotion), 403 → toast + refresh.
- [ ] **Step 5 : Lint + tests front** (`just` recette front / `npm test`).
- [ ] **Step 6 : Commit** `feat(app-partners): bascule de périmètre + formulaire multi-scope (GEN-686)`

---

## Task 6 (T6) : Contract — retrait dual-write + drop colonnes

**Files:**
- Modify: `api/src/pdc/services/dashboard/providers/UsersRepository.ts` (retirer le dual-write)
- Create: `api/src/db/migrations/2026XXXX-drop-user-scope-legacy-columns.sql`

- [ ] **Step 1 : Vérifier** qu'aucun code ne lit `auth.users.operator_id/territory_id` : `grep -rn "operator_id\|territory_id" api/src | grep -i "auth.users\|u\.\(operator\|territory\)_id"` → seuls les writes dual-write subsistent.
- [ ] **Step 2 : Retirer le dual-write** de `UsersRepository` (create/update n'écrivent plus les colonnes).
- [ ] **Step 3 : Migration** `ALTER TABLE auth.users DROP COLUMN operator_id, DROP COLUMN territory_id;`
- [ ] **Step 4 : Run** suite d'intégration complète — PASS.
- [ ] **Step 5 : Commit** `chore(auth): drop colonnes user scope legacy (GEN-686)`

---

## Self-review (couverture spec)

- §4 modèle → T1 (table + toutes contraintes + login_siren). ✓
- §5 auth (fail-closed, seam, regenerate) → T2. ✓
- §6 bascule (401/403/typage/tuple complet/purge session) → T3. ✓
- §7 CRUD (gate action, hiérarchie rôle, DISTINCT, promotion défaut, purge) → T4 + UX T5. ✓
- §8 migration (audit, backfill idempotent, dual-write, contrôles bloquants) → T1 (expand) + T6 (contract). ✓
- §9 tests → répartis par tranche. ✓
- Contrats partagés (scopes[], permission, route, repo) → bloc figé en tête. ✓
