import { assertEquals, assertRejects } from "dep:assert";
import { afterAll, beforeAll, describe, it } from "dep:testing-bdd";

import sql from "@/lib/pg/sql.ts";
import { DenoDbContext, makeDenoDbBeforeAfter } from "@/pdc/providers/test/dbMacro.ts";

import { UserScopeRepository } from "./UserScopeRepository.ts";

describe("UserScopeRepository", () => {
  let db: DenoDbContext;
  let repository: UserScopeRepository;
  const { before, after } = makeDenoDbBeforeAfter();
  let seq = 0;
  const seededOperatorId = 1; // maxiCovoit (seeds)

  // Crée un territory_group rattaché à la company seed (dinum, _id=1).
  async function ensureTerritory(id: number): Promise<void> {
    await db.connection.query(sql`
      INSERT INTO territory.territory_group (_id, company_id, name)
      VALUES (${id}::int, 1::int, ${`Test territoire ${id}`}::varchar)
      ON CONFLICT DO NOTHING
    `);
  }

  // Crée un utilisateur nu (sans scope) et renvoie son _id.
  async function createUser(role = "territory.admin"): Promise<number> {
    const email = `user-scope-${++seq}@example.com`;
    const rows = await db.connection.query<{ _id: number }>(sql`
      INSERT INTO auth.users (email, firstname, lastname, password, status, role)
      VALUES (${email}::varchar, 'U'::varchar, 'Scope'::varchar, 'x'::varchar, 'active'::auth.user_status_enum, ${role}::varchar)
      RETURNING _id
    `);
    return rows[0]._id;
  }

  // Utilisateur avec un unique territoire par défaut.
  async function seedUserWithTerritory(territoryId: number): Promise<number> {
    const uid = await createUser();
    await ensureTerritory(territoryId);
    await repository.addTerritory(uid, territoryId, true);
    return uid;
  }

  beforeAll(async () => {
    db = await before();
    repository = new UserScopeRepository(db.connection);
  });

  afterAll(async () => {
    await after(db);
  });

  it("findDefault returns the default scope", async () => {
    const uid = await seedUserWithTerritory(200);
    assertEquals(await repository.findDefault(uid), { territory_id: 200 });
  });

  it("userHasTerritory is false for a non-granted territory", async () => {
    const uid = await seedUserWithTerritory(201);
    assertEquals(await repository.userHasTerritory(uid, 999), false);
    assertEquals(await repository.userHasTerritory(uid, 201), true);
  });

  it("removeTerritory promotes another scope as default", async () => {
    const uid = await createUser();
    await ensureTerritory(202);
    await ensureTerritory(203);
    await repository.addTerritory(uid, 202, true);
    await repository.addTerritory(uid, 203, false);

    await repository.removeTerritory(uid, 202);

    assertEquals(await repository.userHasTerritory(uid, 202), false);
    assertEquals(await repository.findDefault(uid), { territory_id: 203 });
  });

  it("removeTerritory refuses to drop the last scope", async () => {
    const uid = await seedUserWithTerritory(204);
    await assertRejects(() => repository.removeTerritory(uid, 204));
    assertEquals(await repository.userHasTerritory(uid, 204), true);
  });

  it("refuses a second operator scope (single_operator constraint)", async () => {
    const uid = await createUser("operator.admin");
    await repository.setOperator(uid, seededOperatorId);
    await assertRejects(() => repository.setOperator(uid, seededOperatorId));
  });

  it("refuses mixing an operator and a territory scope (homogeneity constraint)", async () => {
    const uid = await createUser("operator.admin");
    await repository.setOperator(uid, seededOperatorId);
    await ensureTerritory(205);
    await assertRejects(() => repository.addTerritory(uid, 205, false));
  });
});
