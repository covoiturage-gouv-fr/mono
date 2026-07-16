# Suppression automatique des comptes back-office inactifs — Plan d'implémentation

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Un command CLI hebdomadaire qui avertit par email les comptes `auth.users` inactifs > 12 mois, puis supprime après 7 jours ceux qui ne se sont pas reconnectés.

**Architecture:** Une colonne `deletion_warned_at` sur `auth.users` sert d'état de contact persistant. Un `@command` idempotent (`dashboard:purge-inactive-users`) fait deux phases par run (avertir / supprimer), pilotées par timestamps. La reconnexion (`authenticateByEmail`) remet le marqueur à NULL et annule le process. La logique SQL vit dans `UsersRepository` (testée en intégration), l'email réutilise le transporter nodemailer/MJML des exports.

**Tech Stack:** Deno 2.x, TypeScript, Inversify (ILOS), Postgres, nodemailer + MJML.

## Global Constraints

- Cible : rôles `operator.*` et `territory.*` uniquement. **Exclure `registry.*`** (équipe RPC interne) via `role NOT LIKE 'registry.%'` dans TOUTE requête de sélection.
- Copie email en français, ton du mail déjà validé (voir spec `docs/superpowers/specs/2026-07-16-purge-inactive-users-design.md`).
- Bouton de reconnexion → `https://partenaire.covoiturage.beta.gouv.fr` (espace partenaire).
- **Ne poser `deletion_warned_at` qu'après un envoi email réellement effectué** ; sur échec, ne pas marquer (repris au run suivant).
- Requêtes paramétrées via `@/lib/pg/sql.ts` (`sql`, `raw`, `join`) — les valeurs `${x}` deviennent des params `$n` ; les intervalles se passent en param casté `${x}::interval`.
- Mode undercover : aucune mention Claude/Anthropic dans commits/PR/trailers.
- Commits signés (Yubikey) par l'humain ; jamais sur `main` ; worktree + PR.
- Tests : unit `*.unit.spec.ts` (`just tu`), intégration `*.integration.spec.ts` (`just ti`, stack requise). Import assert = `dep:assert`, BDD = `dep:testing-bdd`.

---

## File Structure

- `api/src/db/migrations/20260716120000-add-deletion-warned-at-to-users.sql` — **créer** : migration colonne.
- `api/src/pdc/services/dashboard/providers/UsersRepository.ts` — **modifier** : 3 méthodes (`findUsersToWarn`, `markUserWarned`, `findUsersToDelete`).
- `api/src/pdc/services/dashboard/interfaces/UsersRepositoryInterface.ts` — **modifier** : signatures + resolver.
- `api/src/pdc/services/dashboard/providers/UsersRepository.purge.integration.spec.ts` — **créer** : tests intégration sélection.
- `api/src/pdc/services/auth/providers/UserRepository.ts` — **modifier** : reset marqueur dans `authenticateByEmail`.
- `api/src/pdc/services/auth/providers/UserRepository.reset-marker.integration.spec.ts` — **créer** : test reset.
- `api/src/pdc/services/dashboard/notifications/InactiveUserDeletionWarningNotification.ts` — **créer** : notification email.
- `api/src/pdc/services/dashboard/notifications/InactiveUserDeletionWarningNotification.unit.spec.ts` — **créer**.
- `api/src/pdc/services/dashboard/commands/PurgeInactiveUsersCommand.ts` — **créer** : command + guard.
- `api/src/pdc/services/dashboard/commands/PurgeInactiveUsersCommand.unit.spec.ts` — **créer** : test guard.
- `api/src/pdc/services/dashboard/DashboardServiceProvider.ts` — **modifier** : binder notification + enregistrer command.

---

### Task 1: Migration `deletion_warned_at`

**Files:**
- Create: `api/src/db/migrations/20260716120000-add-deletion-warned-at-to-users.sql`
- Test: `api/src/pdc/services/dashboard/providers/UsersRepository.purge.integration.spec.ts` (bloc « schema »)

**Interfaces:**
- Produces: colonne `auth.users.deletion_warned_at timestamptz` (nullable). Consommée par toutes les tâches suivantes.

- [ ] **Step 1: Écrire la migration**

```sql
-- api/src/db/migrations/20260716120000-add-deletion-warned-at-to-users.sql
ALTER TABLE auth.users ADD COLUMN IF NOT EXISTS deletion_warned_at timestamptz;
```

- [ ] **Step 2: Écrire le test « colonne présente »**

```ts
// UsersRepository.purge.integration.spec.ts
import { assertEquals } from "dep:assert";
import { afterAll, beforeAll, describe, it } from "dep:testing-bdd";
import sql from "@/lib/pg/sql.ts";
import { DenoDbContext, makeDenoDbBeforeAfter } from "@/pdc/providers/test/index.ts";

describe("auth.users deletion_warned_at", () => {
  let db: DenoDbContext;
  const { before, after } = makeDenoDbBeforeAfter();
  beforeAll(async () => { db = await before(); });
  afterAll(async () => { await after(db); });

  it("column exists and is nullable", async () => {
    const rows = await db.connection.query<{ is_nullable: string }>(sql`
      SELECT is_nullable FROM information_schema.columns
      WHERE table_schema = 'auth' AND table_name = 'users'
        AND column_name = 'deletion_warned_at'`);
    assertEquals(rows.length, 1);
    assertEquals(rows[0].is_nullable, "YES");
  });
});
```

- [ ] **Step 3: Lancer le test → vérifier qu'il passe (migration appliquée par le harness de test)**

Run: `cd api && just ti '**/UsersRepository.purge.integration.spec.ts'`
Expected: PASS (le harness `makeDenoDbBeforeAfter` recrée la base de test à partir des migrations, colonne présente).

- [ ] **Step 4: Commit**

```bash
git add api/src/db/migrations/20260716120000-add-deletion-warned-at-to-users.sql \
        api/src/pdc/services/dashboard/providers/UsersRepository.purge.integration.spec.ts
git commit -m "feat(dashboard): ajoute la colonne deletion_warned_at sur auth.users"
```

---

### Task 2: Méthodes de sélection dans `UsersRepository`

**Files:**
- Modify: `api/src/pdc/services/dashboard/interfaces/UsersRepositoryInterface.ts`
- Modify: `api/src/pdc/services/dashboard/providers/UsersRepository.ts`
- Test: `api/src/pdc/services/dashboard/providers/UsersRepository.purge.integration.spec.ts`

**Interfaces:**
- Consumes: colonne `deletion_warned_at` (Task 1).
- Produces :
  - `type InactiveUserToWarn = { _id: number; email: string; firstname: string | null; lastname: string | null }`
  - `findUsersToWarn(inactivity: string): Promise<InactiveUserToWarn[]>`
  - `markUserWarned(id: number): Promise<void>`
  - `findUsersToDelete(grace: string): Promise<Array<{ _id: number }>>`
  - `inactivity` / `grace` = chaînes d'intervalle Postgres (ex. `"12 months"`, `"7 days"`).

- [ ] **Step 1: Écrire les tests d'intégration**

Ajouter dans `UsersRepository.purge.integration.spec.ts`, après le bloc « schema » :

```ts
import { UsersRepository } from "./UsersRepository.ts";

describe("UsersRepository purge selection", () => {
  let db: DenoDbContext;
  let repository: UsersRepository;
  const { before, after } = makeDenoDbBeforeAfter();

  beforeAll(async () => {
    db = await before();
    repository = new UsersRepository(db.connection);
    // A: à avertir (operator, inactif 13 mois, jamais averti)
    await db.connection.query(sql`INSERT INTO auth.users (email, firstname, lastname, role, last_login_at)
      VALUES ('warn@purge.test', 'A', 'A', 'operator.user', now() - '13 months'::interval)`);
    // B: registry exclu (inactif mais registry.admin)
    await db.connection.query(sql`INSERT INTO auth.users (email, firstname, lastname, role, last_login_at)
      VALUES ('registry@purge.test', 'B', 'B', 'registry.admin', now() - '13 months'::interval)`);
    // C: récent, non éligible
    await db.connection.query(sql`INSERT INTO auth.users (email, firstname, lastname, role, last_login_at)
      VALUES ('recent@purge.test', 'C', 'C', 'territory.user', now() - '1 month'::interval)`);
    // D: à supprimer (averti il y a 8 jours, pas reconnecté)
    await db.connection.query(sql`INSERT INTO auth.users (email, firstname, lastname, role, last_login_at, deletion_warned_at)
      VALUES ('delete@purge.test', 'D', 'D', 'operator.user', now() - '13 months'::interval, now() - '8 days'::interval)`);
    // E: averti il y a 8 jours MAIS reconnecté depuis (last_login > warned) → survit
    await db.connection.query(sql`INSERT INTO auth.users (email, firstname, lastname, role, last_login_at, deletion_warned_at)
      VALUES ('reconnected@purge.test', 'E', 'E', 'operator.user', now() - '1 hour'::interval, now() - '8 days'::interval)`);
  });
  afterAll(async () => { await after(db); });

  it("findUsersToWarn: inactifs non-registry jamais avertis", async () => {
    const rows = await repository.findUsersToWarn("12 months");
    const emails = rows.map((r) => r.email);
    assertEquals(emails.includes("warn@purge.test"), true);
    assertEquals(emails.includes("registry@purge.test"), false);
    assertEquals(emails.includes("recent@purge.test"), false);
    assertEquals(emails.includes("delete@purge.test"), false); // déjà averti (warned_at non null)
  });

  it("markUserWarned: pose deletion_warned_at", async () => {
    const [{ _id }] = await db.connection.query<{ _id: number }>(sql`
      SELECT _id FROM auth.users WHERE email = 'warn@purge.test'`);
    await repository.markUserWarned(_id);
    const rows = await db.connection.query<{ deletion_warned_at: string | null }>(sql`
      SELECT deletion_warned_at FROM auth.users WHERE _id = ${_id}`);
    assertEquals(rows[0].deletion_warned_at !== null, true);
  });

  it("findUsersToDelete: avertis > grace et non reconnectés, hors registry", async () => {
    const rows = await repository.findUsersToDelete("7 days");
    const ids = new Set(rows.map((r) => r._id));
    const [{ _id: delId }] = await db.connection.query<{ _id: number }>(sql`
      SELECT _id FROM auth.users WHERE email = 'delete@purge.test'`);
    const [{ _id: recoId }] = await db.connection.query<{ _id: number }>(sql`
      SELECT _id FROM auth.users WHERE email = 'reconnected@purge.test'`);
    assertEquals(ids.has(delId), true);
    assertEquals(ids.has(recoId), false); // reconnecté depuis l'avertissement
  });
});
```

- [ ] **Step 2: Lancer les tests → vérifier qu'ils échouent**

Run: `cd api && just ti '**/UsersRepository.purge.integration.spec.ts'`
Expected: FAIL (`repository.findUsersToWarn is not a function`).

- [ ] **Step 3: Ajouter les signatures à l'interface**

Dans `UsersRepositoryInterface.ts`, ajouter le type exporté et les 3 méthodes à `UsersRepositoryInterface` **et** à `UsersRepositoryInterfaceResolver` :

```ts
export type InactiveUserToWarn = {
  _id: number;
  email: string;
  firstname: string | null;
  lastname: string | null;
};

// dans interface UsersRepositoryInterface { ... }
  findUsersToWarn(inactivity: string): Promise<InactiveUserToWarn[]>;
  markUserWarned(id: number): Promise<void>;
  findUsersToDelete(grace: string): Promise<Array<{ _id: number }>>;

// dans abstract class UsersRepositoryInterfaceResolver { ... }
  async findUsersToWarn(_inactivity: string): Promise<InactiveUserToWarn[]> { throw new Error(); }
  async markUserWarned(_id: number): Promise<void> { throw new Error(); }
  async findUsersToDelete(_grace: string): Promise<Array<{ _id: number }>> { throw new Error(); }
```

- [ ] **Step 4: Implémenter les méthodes**

Dans `UsersRepository.ts`, importer le type et ajouter les méthodes (le fichier importe déjà `sql, { join, raw }` et `raw(this.table)`) :

```ts
// ajouter InactiveUserToWarn à l'import depuis UsersRepositoryInterface.ts

async findUsersToWarn(inactivity: string): Promise<InactiveUserToWarn[]> {
  return await this.pgConnection.query<InactiveUserToWarn>(sql`
    SELECT _id, email, firstname, lastname
    FROM ${raw(this.table)}
    WHERE last_login_at < now() - ${inactivity}::interval
      AND deletion_warned_at IS NULL
      AND role NOT LIKE 'registry.%'
  `);
}

async markUserWarned(id: number): Promise<void> {
  await this.pgConnection.query(sql`
    UPDATE ${raw(this.table)} SET deletion_warned_at = now() WHERE _id = ${id}
  `);
}

async findUsersToDelete(grace: string): Promise<Array<{ _id: number }>> {
  return await this.pgConnection.query<{ _id: number }>(sql`
    SELECT _id
    FROM ${raw(this.table)}
    WHERE deletion_warned_at < now() - ${grace}::interval
      AND last_login_at < deletion_warned_at
      AND role NOT LIKE 'registry.%'
  `);
}
```

- [ ] **Step 5: Lancer les tests → vérifier qu'ils passent**

Run: `cd api && just ti '**/UsersRepository.purge.integration.spec.ts'`
Expected: PASS (4 tests).

- [ ] **Step 6: Commit**

```bash
git add api/src/pdc/services/dashboard/interfaces/UsersRepositoryInterface.ts \
        api/src/pdc/services/dashboard/providers/UsersRepository.ts \
        api/src/pdc/services/dashboard/providers/UsersRepository.purge.integration.spec.ts
git commit -m "feat(dashboard): sélection des comptes inactifs à avertir/supprimer"
```

---

### Task 3: Reset du marqueur à la reconnexion

**Files:**
- Modify: `api/src/pdc/services/auth/providers/UserRepository.ts:50`
- Test: `api/src/pdc/services/auth/providers/UserRepository.reset-marker.integration.spec.ts`

**Interfaces:**
- Consumes: colonne `deletion_warned_at` (Task 1).
- Produces: après `authenticateByEmail(email)`, la ligne a `deletion_warned_at = NULL` et `last_login_at = now()`.

- [ ] **Step 1: Écrire le test d'intégration**

```ts
// UserRepository.reset-marker.integration.spec.ts
import { assertEquals } from "dep:assert";
import { afterAll, beforeAll, describe, it } from "dep:testing-bdd";
import sql from "@/lib/pg/sql.ts";
import { DenoDbContext, makeDenoDbBeforeAfter } from "@/pdc/providers/test/index.ts";
import { UserRepository } from "./UserRepository.ts";

describe("UserRepository reset deletion marker on login", () => {
  let db: DenoDbContext;
  let repository: UserRepository;
  const { before, after } = makeDenoDbBeforeAfter();

  beforeAll(async () => {
    db = await before();
    repository = new UserRepository(db.connection);
    await db.connection.query(sql`INSERT INTO auth.users (email, firstname, lastname, role, last_login_at, deletion_warned_at)
      VALUES ('login@reset.test', 'L', 'L', 'operator.user', now() - '13 months'::interval, now() - '2 days'::interval)`);
  });
  afterAll(async () => { await after(db); });

  it("clears deletion_warned_at and refreshes last_login_at", async () => {
    await repository.authenticateByEmail("login@reset.test");
    const rows = await db.connection.query<{ deletion_warned_at: string | null; recent: boolean }>(sql`
      SELECT deletion_warned_at, last_login_at > now() - '1 minute'::interval AS recent
      FROM auth.users WHERE email = 'login@reset.test'`);
    assertEquals(rows[0].deletion_warned_at, null);
    assertEquals(rows[0].recent, true);
  });
});
```

- [ ] **Step 2: Lancer le test → vérifier qu'il échoue**

Run: `cd api && just ti '**/UserRepository.reset-marker.integration.spec.ts'`
Expected: FAIL (`deletion_warned_at` toujours non-null).

- [ ] **Step 3: Implémenter le reset**

Dans `UserRepository.ts` (`raw` est déjà importé ligne 3), modifier la construction des champs de l'UPDATE :

```ts
// avant :
const fields = fromObject({ ...data, last_login_at: "NOW()" });
// après :
const fields = fromObject({ ...data, last_login_at: "NOW()", deletion_warned_at: raw("NULL") });
```

- [ ] **Step 4: Lancer le test → vérifier qu'il passe**

Run: `cd api && just ti '**/UserRepository.reset-marker.integration.spec.ts'`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add api/src/pdc/services/auth/providers/UserRepository.ts \
        api/src/pdc/services/auth/providers/UserRepository.reset-marker.integration.spec.ts
git commit -m "feat(auth): la reconnexion annule le process de suppression (reset marqueur)"
```

---

### Task 4: Notification email d'avertissement

**Files:**
- Create: `api/src/pdc/services/dashboard/notifications/InactiveUserDeletionWarningNotification.ts`
- Test: `api/src/pdc/services/dashboard/notifications/InactiveUserDeletionWarningNotification.unit.spec.ts`

**Interfaces:**
- Consumes: `DefaultNotification`, `DefaultTemplateData` de `@/pdc/providers/notification/index.ts`.
- Produces: `class InactiveUserDeletionWarningNotification extends DefaultNotification` ; `constructor(to: string, data: Partial<InactiveUserDeletionWarningTemplateData>)` ; static `subject`. Le template `DefaultNotification` gère déjà « Bonjour {{fullname}} », le bouton (`action_message`/`action_href`) et le pied de page contact — la copie ne les répète pas.

- [ ] **Step 1: Écrire le test unitaire**

```ts
import { assertEquals, assertStringIncludes } from "dep:assert";
import { describe, it } from "dep:testing-bdd";
import { InactiveUserDeletionWarningNotification } from "./InactiveUserDeletionWarningNotification.ts";

describe("InactiveUserDeletionWarningNotification", () => {
  it("porte le sujet, le CTA et le lien de reconnexion", () => {
    const n = new InactiveUserDeletionWarningNotification("Jane Doe <jane@example.com>", {
      fullname: "Jane Doe",
      action_href: "https://partenaire.covoiturage.beta.gouv.fr",
    });
    assertEquals(
      (n.constructor as typeof InactiveUserDeletionWarningNotification).subject,
      "Votre compte Registre de preuve de covoiturage va être supprimé",
    );
    assertEquals(n.to, "Jane Doe <jane@example.com>");
    assertEquals(n.data.action_message, "Je me connecte au RPC");
    assertEquals(n.data.action_href, "https://partenaire.covoiturage.beta.gouv.fr");
    assertStringIncludes(n.data.message_text ?? "", "RGPD");
    assertStringIncludes(n.data.message_text ?? "", "7 jours");
  });
});
```

- [ ] **Step 2: Lancer le test → vérifier qu'il échoue**

Run: `cd api && just tu '**/InactiveUserDeletionWarningNotification.unit.spec.ts'`
Expected: FAIL (module introuvable).

- [ ] **Step 3: Implémenter la notification**

```ts
// InactiveUserDeletionWarningNotification.ts
import { DefaultNotification, DefaultTemplateData } from "@/pdc/providers/notification/index.ts";

export interface InactiveUserDeletionWarningTemplateData extends DefaultTemplateData {
  action_href: string;
}

const message_html = `
<p>Nous avons remarqué que vous n'avez pas utilisé votre compte Registre de preuve de covoiturage depuis plus d'un an. Pour rappel, celui-ci vous donne accès à votre espace partenaire pour effectuer des exports de données ou consulter une campagne d'incitations financières.</p>
<p>Conformément à notre politique de confidentialité et aux exigences du Règlement Général sur la Protection des Données (RGPD), nous tenons à vous informer que votre compte sera automatiquement supprimé dans 7 jours si aucune activité n'est détectée d'ici là.</p>
<p>Si vous souhaitez conserver votre compte et l'accès à votre espace partenaire, reconnectez-vous en cliquant sur le bouton ci-dessous.</p>
<p>En cas de suppression, toutes vos données personnelles seront définitivement effacées du Registre de preuve de covoiturage. Vous ne serez pas supprimé des newsletters auxquelles vous avez pu vous inscrire.</p>
`;

const message_text = `Nous avons remarqué que vous n'avez pas utilisé votre compte Registre de preuve de covoiturage depuis plus d'un an. Pour rappel, celui-ci vous donne accès à votre espace partenaire pour effectuer des exports de données ou consulter une campagne d'incitations financières.

Conformément à notre politique de confidentialité et aux exigences du Règlement Général sur la Protection des Données (RGPD), votre compte sera automatiquement supprimé dans 7 jours si aucune activité n'est détectée d'ici là.

Si vous souhaitez conserver votre compte, reconnectez-vous via le lien ci-dessous.

En cas de suppression, toutes vos données personnelles seront définitivement effacées. Vous ne serez pas supprimé des newsletters auxquelles vous avez pu vous inscrire.`;

const defaultData: Partial<InactiveUserDeletionWarningTemplateData> = {
  title: "Votre compte va être supprimé",
  preview: "Votre compte Registre de preuve de covoiturage sera supprimé dans 7 jours.",
  action_message: "Je me connecte au RPC",
  message_html,
  message_text,
};

export class InactiveUserDeletionWarningNotification extends DefaultNotification {
  static override readonly subject = "Votre compte Registre de preuve de covoiturage va être supprimé";
  constructor(to: string, data: Partial<InactiveUserDeletionWarningTemplateData>) {
    super(to, { ...defaultData, ...data });
  }
}
```

- [ ] **Step 4: Lancer le test → vérifier qu'il passe**

Run: `cd api && just tu '**/InactiveUserDeletionWarningNotification.unit.spec.ts'`
Expected: PASS.

- [ ] **Step 5: Commit**

```bash
git add api/src/pdc/services/dashboard/notifications/InactiveUserDeletionWarningNotification.ts \
        api/src/pdc/services/dashboard/notifications/InactiveUserDeletionWarningNotification.unit.spec.ts
git commit -m "feat(dashboard): email d'avertissement avant suppression de compte inactif"
```

---

### Task 5: Command `dashboard:purge-inactive-users` + câblage

**Files:**
- Create: `api/src/pdc/services/dashboard/commands/PurgeInactiveUsersCommand.ts`
- Test: `api/src/pdc/services/dashboard/commands/PurgeInactiveUsersCommand.unit.spec.ts`
- Modify: `api/src/pdc/services/dashboard/DashboardServiceProvider.ts`

**Interfaces:**
- Consumes: `UsersRepositoryInterfaceResolver` (Task 2 : `findUsersToWarn`, `markUserWarned`, `findUsersToDelete`, `deleteUser`), `InactiveUserDeletionWarningNotification` (Task 4), `NotificationTransporterInterfaceResolver`, `ConfigInterfaceResolver`.
- Produces: signature CLI `dashboard:purge-inactive-users` ; fonction pure exportée `assertNotificationConfigured(config)`.

- [ ] **Step 1: Écrire le test unitaire du guard**

```ts
// PurgeInactiveUsersCommand.unit.spec.ts
import { assertThrows } from "dep:assert";
import { describe, it } from "dep:testing-bdd";
import { ConfigInterfaceResolver } from "@/ilos/common/index.ts";
import { assertNotificationConfigured } from "./PurgeInactiveUsersCommand.ts";

function fakeConfig(smtp: unknown): ConfigInterfaceResolver {
  return { get: (_k: string, d?: unknown) => smtp ?? d } as unknown as ConfigInterfaceResolver;
}

describe("assertNotificationConfigured", () => {
  it("throws quand la config SMTP est absente", () => {
    assertThrows(() => assertNotificationConfigured(fakeConfig(null)));
  });
  it("ne throw pas quand la config SMTP est présente", () => {
    assertNotificationConfigured(fakeConfig({ host: "smtp.example.com" }));
  });
});
```

- [ ] **Step 2: Lancer le test → vérifier qu'il échoue**

Run: `cd api && just tu '**/PurgeInactiveUsersCommand.unit.spec.ts'`
Expected: FAIL (module introuvable).

- [ ] **Step 3: Implémenter le command**

```ts
// PurgeInactiveUsersCommand.ts
import { command, CommandInterface, ConfigInterfaceResolver } from "@/ilos/common/index.ts";
import { logger } from "@/lib/logger/index.ts";
import {
  MailTemplateNotificationInterface,
  NotificationTransporterInterfaceResolver,
} from "@/pdc/providers/notification/index.ts";
import { UsersRepositoryInterfaceResolver } from "@/pdc/services/dashboard/interfaces/UsersRepositoryInterface.ts";
import { InactiveUserDeletionWarningNotification } from "../notifications/InactiveUserDeletionWarningNotification.ts";

interface Options {
  inactivity: string;
  grace: string;
  appUrl: string;
  dryRun: boolean;
}

// Garde-fou : le transporter mail no-op silencieusement si non configuré.
// On refuse de tourner sans SMTP, sinon on marquerait/supprimerait des comptes jamais avertis.
export function assertNotificationConfigured(config: ConfigInterfaceResolver): void {
  const smtp = config.get("notification.mail.smtp", null);
  if (!smtp) {
    throw new Error(
      "[purge-inactive-users] notification.mail.smtp non configuré ; arrêt pour éviter de supprimer des comptes non avertis",
    );
  }
}

@command({
  signature: "dashboard:purge-inactive-users",
  description: "Avertit puis supprime les comptes back-office inactifs (RGPD)",
  options: [
    { signature: "--inactivity <inactivity>", description: "Seuil d'inactivité (interval PG)", default: "12 months" },
    { signature: "--grace <grace>", description: "Durée du préavis (interval PG)", default: "7 days" },
    {
      signature: "--app-url <appUrl>",
      description: "URL de reconnexion (espace partenaire)",
      default: "https://partenaire.covoiturage.beta.gouv.fr",
    },
    { signature: "--dry-run", description: "N'envoie rien et ne supprime rien, log seulement" },
  ],
})
export class PurgeInactiveUsersCommand implements CommandInterface {
  constructor(
    private config: ConfigInterfaceResolver,
    private usersRepository: UsersRepositoryInterfaceResolver,
    private emailer: NotificationTransporterInterfaceResolver<MailTemplateNotificationInterface>,
  ) {}

  public async call(options: Options): Promise<string> {
    assertNotificationConfigured(this.config);

    // Phase 1 — avertir
    const toWarn = await this.usersRepository.findUsersToWarn(options.inactivity);
    let warned = 0;
    for (const user of toWarn) {
      const fullname = `${user.firstname ?? ""} ${user.lastname ?? ""}`.trim() || user.email;
      try {
        if (options.dryRun) {
          logger.info(`[purge-inactive-users] (dry-run) avertirait ${user._id}`);
        } else {
          await this.emailer.send(
            new InactiveUserDeletionWarningNotification(`${fullname} <${user.email}>`, {
              fullname,
              action_href: options.appUrl,
            }),
          );
          await this.usersRepository.markUserWarned(user._id);
        }
        warned++;
      } catch (e) {
        logger.error(`[purge-inactive-users] échec avertissement ${user._id}: ${e instanceof Error ? e.message : e}`);
      }
    }

    // Phase 2 — supprimer
    const toDelete = await this.usersRepository.findUsersToDelete(options.grace);
    let deleted = 0;
    for (const user of toDelete) {
      try {
        if (options.dryRun) {
          logger.info(`[purge-inactive-users] (dry-run) supprimerait ${user._id}`);
        } else {
          await this.usersRepository.deleteUser({ id: user._id });
          deleted++;
        }
      } catch (e) {
        logger.error(`[purge-inactive-users] échec suppression ${user._id}: ${e instanceof Error ? e.message : e}`);
      }
    }

    const summary =
      `[purge-inactive-users] avertis=${warned}/${toWarn.length} supprimés=${deleted}/${toDelete.length}` +
      (options.dryRun ? " (dry-run)" : "");
    logger.info(summary);
    return summary;
  }
}
```

- [ ] **Step 4: Lancer le test → vérifier qu'il passe**

Run: `cd api && just tu '**/PurgeInactiveUsersCommand.unit.spec.ts'`
Expected: PASS.

- [ ] **Step 5: Câbler le service provider**

Dans `DashboardServiceProvider.ts` : importer, binder le transporter mail, enregistrer le command.

```ts
import { defaultNotificationBindings } from "../../providers/notification/index.ts";
import { PurgeInactiveUsersCommand } from "./commands/PurgeInactiveUsersCommand.ts";

// commands: [] -> commands: [PurgeInactiveUsersCommand]
// providers: [...] -> ajouter ...defaultNotificationBindings au tableau providers
```

Résultat attendu du bloc `@serviceProvider` :

```ts
  commands: [PurgeInactiveUsersCommand],
  providers: [
    S3StorageProvider,
    BrevoProvider,
    OperatorsRepository,
    TerritoriesRepository,
    JourneysRepository,
    CampaignsRepository,
    UsersRepository,
    ...defaultNotificationBindings,
  ],
```

- [ ] **Step 6: Vérifier le boot + dry-run (stack requise)**

Run: `cd api && just cli "dashboard:purge-inactive-users --dry-run"`
Expected: le service démarre, log `[purge-inactive-users] avertis=.../... supprimés=.../... (dry-run)`, aucune écriture en base. Si erreur de binding notification → vérifier `...defaultNotificationBindings`. Si `notification.mail.smtp non configuré` → renseigner la config SMTP locale avant réessai.

- [ ] **Step 7: Commit**

```bash
git add api/src/pdc/services/dashboard/commands/PurgeInactiveUsersCommand.ts \
        api/src/pdc/services/dashboard/commands/PurgeInactiveUsersCommand.unit.spec.ts \
        api/src/pdc/services/dashboard/DashboardServiceProvider.ts
git commit -m "feat(dashboard): command purge-inactive-users (avertir puis supprimer, RGPD)"
```

---

## Hors-scope de ce plan (à faire séparément)

- **CronJob k8s hebdomadaire** lançant `deno run src/main.ts dashboard:purge-inactive-users` (hébergement = cluster k8s). Aucun scheduler in-app dans le repo ; se configure côté manifests k8s.
- Documenter la politique de rétention (registre des traitements RGPD).
- Premier run réel précédé d'un `--dry-run` en prod pour valider le périmètre.

## Self-Review

- **Couverture spec** : colonne (T1) ✓ ; sélection avertir/supprimer + exclusion registry + non-reconnecté (T2) ✓ ; reset au login (T3) ✓ ; email FR + CTA reconnexion (T4) ✓ ; command idempotent, guard SMTP, dry-run, seuils en options (T5) ✓ ; trigger hebdo → hors-scope (infra) ✓.
- **Placeholders** : aucun ; toutes les valeurs (URL partenaire, intervalles, copie) sont concrètes.
- **Cohérence des types** : `findUsersToWarn/markUserWarned/findUsersToDelete` identiques entre interface (T2), repo (T2) et command (T5) ; `InactiveUserDeletionWarningNotification(to, {fullname, action_href})` identique entre T4 et T5.
