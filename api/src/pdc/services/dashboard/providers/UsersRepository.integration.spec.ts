import { assertEquals, assertRejects } from "dep:assert";
import { afterAll, beforeAll, describe, it } from "dep:testing-bdd";
import { NotFoundException } from "@/ilos/common/index.ts";
import sql from "@/lib/pg/sql.ts";
import { DenoDbContext, makeDenoDbBeforeAfter } from "@/pdc/providers/test/index.ts";
import { UserScopeRepository } from "@/pdc/services/auth/providers/UserScopeRepository.ts";
import { UsersRepository } from "./UsersRepository.ts";

describe("UsersRepository", () => {
  let db: DenoDbContext;
  let repository: UsersRepository;
  const { before, after } = makeDenoDbBeforeAfter();

  let createdUserId: number;

  let scopeRepository: UserScopeRepository;

  beforeAll(async () => {
    db = await before();
    scopeRepository = new UserScopeRepository(db.connection);
    repository = new UsersRepository(db.connection, scopeRepository);
  });

  afterAll(async () => {
    await after(db);
  });

  it("Should create a user", async () => {
    const userData = {
      firstname: "John",
      lastname: "Doe",
      email: "john.doe@example.com",
      role: "registry.admin" as const,
      operator_id: null,
      territory_id: null,
    };

    const result = await repository.createUser(userData);

    assertEquals(result.success, true);
    assertEquals(typeof result.message, "string");

    // Verify the user was created by fetching it
    const users = await repository.getUsers({ search: "john.doe@example.com" });
    assertEquals(users.data.length, 1);
    assertEquals(users.data[0].firstname, "John");
    assertEquals(users.data[0].lastname, "Doe");
    assertEquals(users.data[0].email, "john.doe@example.com");
    assertEquals(users.data[0].role, "registry.admin");

    createdUserId = users.data[0].id;
  });

  it("Should get users with pagination", async () => {
    // Create additional users
    await repository.createUser({
      firstname: "Jane",
      lastname: "Smith",
      email: "jane.smith@example.com",
      role: "operator.admin" as const,
      operator_id: null,
      territory_id: null,
    });

    await repository.createUser({
      firstname: "Bob",
      lastname: "Wilson",
      email: "bob.wilson@example.com",
      role: "territory.admin" as const,
      operator_id: null,
      territory_id: null,
    });

    // Test pagination
    const page1 = await repository.getUsers({ limit: 2, page: 1 });
    assertEquals(page1.data.length, 2);
    assertEquals(page1.meta.page, 1);
    assertEquals(page1.meta.total >= 3, true);

    const page2 = await repository.getUsers({ limit: 2, page: 2 });
    assertEquals(page2.meta.page, 2);
  });

  it("Should get users by id", async () => {
    const result = await repository.getUsers({ id: createdUserId });

    assertEquals(result.data.length, 1);
    assertEquals(result.data[0].id, createdUserId);
    assertEquals(result.data[0].firstname, "John");
  });

  it("Should search users by name", async () => {
    const result = await repository.getUsers({ search: "Jane" });

    assertEquals(result.data.length, 1);
    assertEquals(result.data[0].firstname, "Jane");
    assertEquals(result.data[0].lastname, "Smith");
  });

  it("Should search users by email", async () => {
    const result = await repository.getUsers({ search: "bob.wilson" });

    assertEquals(result.data.length, 1);
    assertEquals(result.data[0].email, "bob.wilson@example.com");
  });

  it("Should update a user", async () => {
    const updateData = {
      id: createdUserId,
      firstname: "Johnny",
      lastname: "Updated",
      email: "johnny.updated@example.com",
      role: "operator.user" as const,
      operator_id: null,
      territory_id: null,
    };

    const result = await repository.updateUser(updateData);

    assertEquals(result.success, true);
    assertEquals(typeof result.message, "string");

    // Verify the update
    const users = await repository.getUsers({ id: createdUserId });
    assertEquals(users.data.length, 1);
    assertEquals(users.data[0].firstname, "Johnny");
    assertEquals(users.data[0].lastname, "Updated");
    assertEquals(users.data[0].email, "johnny.updated@example.com");
    assertEquals(users.data[0].role, "operator.user");
  });

  it("Should fail to update a non-existent user", async () => {
    const updateData = {
      id: 999999,
      firstname: "NonExistent",
      lastname: "User",
      email: "nonexistent@example.com",
      role: "registry.admin" as const,
      operator_id: null,
      territory_id: null,
    };

    await assertRejects(
      async () => await repository.updateUser(updateData),
      Error,
      "Unable to update user",
    );
  });

  it("Should delete a user", async () => {
    const result = await repository.deleteUser({ id: createdUserId });

    assertEquals(result.success, true);
    assertEquals(result.outcome, "user_deleted");
    assertEquals(result.message, `user ${createdUserId} deleted`);

    // Verify deletion
    const users = await repository.getUsers({ id: createdUserId });
    assertEquals(users.data.length, 0);
  });

  it("Should fail to delete a non-existent user", async () => {
    await assertRejects(
      async () => await repository.deleteUser({ id: 999999 }),
      NotFoundException,
      "Not found",
    );
  });

  it("Should delete remaining test users", async () => {
    // Clean up the other users we created
    const janeUsers = await repository.getUsers({ search: "jane.smith@example.com" });
    if (janeUsers.data.length > 0) {
      await repository.deleteUser({ id: janeUsers.data[0].id });
    }

    const bobUsers = await repository.getUsers({ search: "bob.wilson@example.com" });
    if (bobUsers.data.length > 0) {
      await repository.deleteUser({ id: bobUsers.data[0].id });
    }
  });
});

describe("UsersRepository multi-scope (pivot)", () => {
  let db: DenoDbContext;
  let repository: UsersRepository;
  const { before, after } = makeDenoDbBeforeAfter();

  async function ensureTerritory(id: number): Promise<void> {
    await db.connection.query(sql`
      INSERT INTO territory.territory_group (_id, company_id, name)
      VALUES (${id}::int, 1::int, ${`Test territoire ${id}`}::varchar)
      ON CONFLICT DO NOTHING
    `);
  }

  beforeAll(async () => {
    db = await before();
    repository = new UsersRepository(db.connection, new UserScopeRepository(db.connection));
    await ensureTerritory(310);
    await ensureTerritory(311);
  });

  afterAll(async () => {
    await after(db);
  });

  it("create writes the pivot (default + extra territories)", async () => {
    await repository.createUser({
      firstname: "Multi",
      lastname: "Scope",
      email: "multi.scope@example.com",
      role: "territory.admin",
      operator_id: null,
      territory_id: 310,
      scopes: [{ territory_id: 310, is_default: true }, { territory_id: 311 }],
    });

    const created = await repository.getUsers({ search: "multi.scope@example.com" });
    assertEquals(created.data.length, 1);
    const uid = created.data[0].id;

    const scopeRows = await db.connection.query<{ n: number; d: number }>(sql`
      SELECT count(*)::int AS n, count(*) FILTER (WHERE is_default)::int AS d
      FROM auth.user_scopes WHERE user_id = ${uid}
    `);
    assertEquals(scopeRows[0].n, 2); // 310 défaut + 311
    assertEquals(scopeRows[0].d, 1); // exactement un défaut
  });

  it("list via pivot shows a multi-territory user only once (DISTINCT)", async () => {
    const byT310 = await repository.getUsers({ territory_id: 310, search: "multi.scope" });
    assertEquals(byT310.data.length, 1);
    assertEquals(byT310.meta.total, 1);

    const byT311 = await repository.getUsers({ territory_id: 311, search: "multi.scope" });
    assertEquals(byT311.data.length, 1);
  });

  it("list exposes the default scope of the pivot", async () => {
    const listed = await repository.getUsers({ search: "multi.scope@example.com" });
    assertEquals(listed.data[0].territory_id, 310);
    assertEquals(listed.data[0].operator_id, null);
  });

  it("scopes_count compte tous les périmètres, même filtré sur un seul", async () => {
    const listed = await repository.getUsers({ search: "multi.scope@example.com" });
    assertEquals(listed.data[0].scopes_count, 2);

    // Filtré sur 310 : le compte n'en expose qu'un, mais en porte bien deux.
    const byT310 = await repository.getUsers({ territory_id: 310, search: "multi.scope" });
    assertEquals(byT310.data[0].scopes_count, 2);
  });

  it("la liste expose les périmètres complets et le login_siren", async () => {
    const listed = await repository.getUsers({ search: "multi.scope@example.com" });
    assertEquals(listed.data[0].scopes, [
      { territory_id: 310, is_default: true },
      { territory_id: 311, is_default: false },
    ]);
    assertEquals(listed.data[0].login_siren, null);
  });

  // Régression : un admin de territoire n'envoie pas scopes[] ; son update ne doit rien amputer.
  it("update sans scopes conserve le pivot", async () => {
    const uid = (await repository.getUsers({ search: "multi.scope@example.com" })).data[0].id;

    await repository.updateUser({
      id: uid,
      firstname: "Multi",
      lastname: "Scope",
      email: "multi.scope@example.com",
      role: "territory.admin",
      operator_id: null,
      territory_id: 310,
    });

    const after = await repository.getUsers({ id: uid });
    assertEquals(after.data[0].scopes_count, 2);
  });

  it("update avec scopes applique le drapeau is_default", async () => {
    const uid = (await repository.getUsers({ search: "multi.scope@example.com" })).data[0].id;

    await repository.updateUser({
      id: uid,
      firstname: "Multi",
      lastname: "Scope",
      email: "multi.scope@example.com",
      role: "territory.admin",
      operator_id: null,
      territory_id: 310,
      scopes: [{ territory_id: 310, is_default: false }, { territory_id: 311, is_default: true }],
    });

    const after = await repository.getUsers({ id: uid });
    assertEquals(after.data[0].territory_id, 311);
    assertEquals(after.data[0].scopes, [
      { territory_id: 311, is_default: true },
      { territory_id: 310, is_default: false },
    ]);

    // Remise en état pour les cas suivants (310 redevient le défaut).
    await repository.updateUser({
      id: uid,
      firstname: "Multi",
      lastname: "Scope",
      email: "multi.scope@example.com",
      role: "territory.admin",
      operator_id: null,
      territory_id: 310,
      scopes: [{ territory_id: 310, is_default: true }, { territory_id: 311 }],
    });
  });

  it("delete scoped on a non-granted territory finds nothing", async () => {
    const created = await repository.getUsers({ search: "multi.scope@example.com" });
    const uid = created.data[0].id;
    await assertRejects(
      async () => await repository.deleteUser({ id: uid, territory_id: 999999 }),
      NotFoundException,
      "Not found",
    );
  });

  it("delete from a territory only releases that scope", async () => {
    const created = await repository.getUsers({ search: "multi.scope@example.com" });
    const uid = created.data[0].id;
    const result = await repository.deleteUser({ id: uid, territory_id: 311 });
    assertEquals(result.outcome, "scope_released");

    const scopeRows = await db.connection.query<{ territory_id: number }>(sql`
      SELECT territory_id FROM auth.user_scopes WHERE user_id = ${uid}
    `);
    assertEquals(scopeRows.length, 1);
    assertEquals(scopeRows[0].territory_id, 310);

    const still = await repository.getUsers({ id: uid });
    assertEquals(still.data.length, 1);
  });

  it("releasing the default scope promotes another one", async () => {
    await repository.createUser({
      firstname: "Promote",
      lastname: "Scope",
      email: "promote.scope@example.com",
      role: "territory.admin" as const,
      operator_id: null,
      territory_id: 310,
      scopes: [{ territory_id: 310, is_default: true }, { territory_id: 311 }],
    });
    const uid = (await repository.getUsers({ search: "promote.scope@example.com" })).data[0].id;

    const result = await repository.deleteUser({ id: uid, territory_id: 310 });
    assertEquals(result.outcome, "scope_released");

    const rows = await db.connection.query<{ territory_id: number; is_default: boolean }>(sql`
      SELECT territory_id, is_default FROM auth.user_scopes WHERE user_id = ${uid}
    `);
    assertEquals(rows.length, 1);
    assertEquals(rows[0].territory_id, 311);
    assertEquals(rows[0].is_default, true);
  });

  it("delete of the last scope removes the account and its pivot rows", async () => {
    const created = await repository.getUsers({ search: "multi.scope@example.com" });
    const uid = created.data[0].id;

    const result = await repository.deleteUser({ id: uid, territory_id: 310 });
    assertEquals(result.outcome, "user_deleted");

    const scopeRows = await db.connection.query<{ n: number }>(sql`
      SELECT count(*)::int AS n FROM auth.user_scopes WHERE user_id = ${uid}
    `);
    assertEquals(scopeRows[0].n, 0);
    assertEquals((await repository.getUsers({ id: uid })).data.length, 0);
  });

  it("operator caller deletes the account (scope 1:1)", async () => {
    await repository.createUser({
      firstname: "Op",
      lastname: "Scope",
      email: "op.scope@example.com",
      role: "operator.admin" as const,
      operator_id: 1,
      territory_id: null,
    });
    const uid = (await repository.getUsers({ search: "op.scope@example.com" })).data[0].id;

    const result = await repository.deleteUser({ id: uid, operator_id: 1 });
    assertEquals(result.outcome, "user_deleted");
    assertEquals((await repository.getUsers({ id: uid })).data.length, 0);
  });
});
