import { assertEquals } from "dep:assert";
import { afterAll, beforeAll, describe, it } from "dep:testing-bdd";

import sql from "@/lib/pg/sql.ts";
import { DenoDbContext, makeDenoDbBeforeAfter } from "@/pdc/providers/test/dbMacro.ts";

import { UserRepository } from "./UserRepository.ts";
import { UserScopeRepository } from "./UserScopeRepository.ts";

describe("UserRepository.authenticateByEmail", () => {
  let db: DenoDbContext;
  let repository: UserRepository;
  let scopes: UserScopeRepository;
  const { before, after } = makeDenoDbBeforeAfter();
  let seq = 0;

  // Crée un territory_group rattaché à la company seed (dinum, _id=1).
  async function ensureTerritory(id: number): Promise<void> {
    await db.connection.query(sql`
      INSERT INTO territory.territory_group (_id, company_id, name)
      VALUES (${id}::int, 1::int, ${`Test territoire ${id}`}::varchar)
      ON CONFLICT DO NOTHING
    `);
  }

  // Crée un utilisateur nu (sans scope) avec un login_siren donné et renvoie {id, email}.
  async function createUser(loginSiren: string | null): Promise<{ id: number; email: string }> {
    const email = `auth-by-email-${++seq}@example.com`;
    const rows = await db.connection.query<{ _id: number }>(sql`
      INSERT INTO auth.users (email, firstname, lastname, password, status, role, login_siren)
      VALUES (${email}::varchar, 'U'::varchar, 'Auth'::varchar, 'x'::varchar, 'active'::auth.user_status_enum, 'territory.admin'::varchar, ${loginSiren}::varchar)
      RETURNING _id
    `);
    return { id: rows[0]._id, email };
  }

  beforeAll(async () => {
    db = await before();
    scopes = new UserScopeRepository(db.connection);
    repository = new UserRepository(db.connection, scopes);
  });

  afterAll(async () => {
    await after(db);
  });

  it("returns login_siren and seeds the active scope from the default territory", async () => {
    const { id, email } = await createUser("123456789");
    await ensureTerritory(300);
    await ensureTerritory(301);
    await scopes.addTerritory(id, 300, true);
    await scopes.addTerritory(id, 301, false);

    const user = await repository.authenticateByEmail(email);

    assertEquals(user?.login_siren, "123456789");
    assertEquals(user?.territory_id, 300);
    assertEquals(user?.operator_id, null);
    assertEquals(user?.scopes.length, 2);
    // défaut en tête (is_default DESC)
    assertEquals(user?.scopes[0].territory_id, 300);
  });

  it("falls back to MIN(_id) scope when no default is flagged", async () => {
    const { id, email } = await createUser(null);
    await ensureTerritory(302);
    await ensureTerritory(303);
    await scopes.addTerritory(id, 302, false);
    await scopes.addTerritory(id, 303, false);

    const user = await repository.authenticateByEmail(email);

    assertEquals(user?.login_siren, null);
    assertEquals(user?.territory_id, 302); // MIN(_id) = premier inséré
    assertEquals(user?.scopes.length, 2);
  });

  it("returns null for an unknown email", async () => {
    assertEquals(await repository.authenticateByEmail("does-not-exist@example.com"), null);
  });
});
