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

  it("create dual-writes the pivot (default + extra territories)", async () => {
    await repository.createUser({
      firstname: "Multi",
      lastname: "Scope",
      email: "multi.scope@example.com",
      role: "territory.admin",
      operator_id: null,
      territory_id: 310,
      scopes: [311],
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

  it("delete cascades the pivot rows", async () => {
    const created = await repository.getUsers({ search: "multi.scope@example.com" });
    const uid = created.data[0].id;
    await repository.deleteUser({ id: uid });
    const scopeRows = await db.connection.query<{ n: number }>(sql`
      SELECT count(*)::int AS n FROM auth.user_scopes WHERE user_id = ${uid}
    `);
    assertEquals(scopeRows[0].n, 0);
  });
});
