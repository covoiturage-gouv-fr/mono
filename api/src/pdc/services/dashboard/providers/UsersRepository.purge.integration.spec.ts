import { assertEquals } from "dep:assert";
import { afterAll, beforeAll, describe, it } from "dep:testing-bdd";
import sql from "@/lib/pg/sql.ts";
import { DenoDbContext, makeDenoDbBeforeAfter } from "@/pdc/providers/test/index.ts";
import { UserScopeRepository } from "@/pdc/services/auth/providers/UserScopeRepository.ts";
import { UsersRepository } from "./UsersRepository.ts";

describe("auth.users deletion_warned_at", () => {
  let db: DenoDbContext;
  const { before, after } = makeDenoDbBeforeAfter();
  beforeAll(async () => {
    db = await before();
  });
  afterAll(async () => {
    await after(db);
  });

  it("column exists and is nullable", async () => {
    const rows = await db.connection.query<{ is_nullable: string }>(sql`
      SELECT is_nullable FROM information_schema.columns
      WHERE table_schema = 'auth' AND table_name = 'users'
        AND column_name = 'deletion_warned_at'`);
    assertEquals(rows.length, 1);
    assertEquals(rows[0].is_nullable, "YES");
  });
});

describe("UsersRepository purge selection", () => {
  let db: DenoDbContext;
  let repository: UsersRepository;
  const { before, after } = makeDenoDbBeforeAfter();

  beforeAll(async () => {
    db = await before();
    repository = new UsersRepository(db.connection, new UserScopeRepository(db.connection));
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
    await db.connection.query(
      sql`INSERT INTO auth.users (email, firstname, lastname, role, last_login_at, deletion_warned_at)
      VALUES ('delete@purge.test', 'D', 'D', 'operator.user', now() - '13 months'::interval, now() - '8 days'::interval)`,
    );
    // E: averti il y a 8 jours MAIS reconnecté depuis (last_login > warned) → survit
    await db.connection.query(
      sql`INSERT INTO auth.users (email, firstname, lastname, role, last_login_at, deletion_warned_at)
      VALUES ('reconnected@purge.test', 'E', 'E', 'operator.user', now() - '1 hour'::interval, now() - '8 days'::interval)`,
    );
    // F: registry averti il y a 8 jours, pas reconnecté → JAMAIS supprimé (exclusion registry sur la requête destructive)
    await db.connection.query(
      sql`INSERT INTO auth.users (email, firstname, lastname, role, last_login_at, deletion_warned_at)
      VALUES ('registry-delete@purge.test', 'F', 'F', 'registry.admin', now() - '13 months'::interval, now() - '8 days'::interval)`,
    );
  });
  afterAll(async () => {
    await after(db);
  });

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
    const [{ _id: registryId }] = await db.connection.query<{ _id: number }>(sql`
      SELECT _id FROM auth.users WHERE email = 'registry-delete@purge.test'`);
    assertEquals(ids.has(delId), true);
    assertEquals(ids.has(recoId), false); // reconnecté depuis l'avertissement
    assertEquals(ids.has(registryId), false); // registry exclu même averti et non reconnecté
  });

  // Doit rester le dernier test du bloc : il supprime réellement des lignes.
  it("deleteInactiveUsers: supprime atomiquement les mêmes cibles et épargne reconnecté/registry", async () => {
    const removed = await repository.deleteInactiveUsers("7 days");
    const removedIds = new Set(removed.map((r) => r._id));

    const remaining = await db.connection.query<{ email: string }>(sql`
      SELECT email FROM auth.users WHERE email IN ('delete@purge.test', 'reconnected@purge.test', 'registry-delete@purge.test')`);
    const remainingEmails = remaining.map((r) => r.email);

    assertEquals(removed.length, 1); // seul delete@purge.test est éligible
    assertEquals(remainingEmails.includes("delete@purge.test"), false); // effectivement supprimé
    assertEquals(remainingEmails.includes("reconnected@purge.test"), true); // reconnecté épargné
    assertEquals(remainingEmails.includes("registry-delete@purge.test"), true); // registry épargné
    assertEquals(removedIds.size, 1);
  });
});
