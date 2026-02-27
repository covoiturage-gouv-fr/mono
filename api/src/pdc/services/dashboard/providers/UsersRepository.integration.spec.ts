import { afterAll, assertEquals, assertRejects, beforeAll, describe, it } from "@/dev_deps.ts";
import { NotFoundException } from "@/ilos/common/index.ts";
import { DenoDbContext, makeDenoDbBeforeAfter } from "@/pdc/providers/test/index.ts";
import { UsersRepository } from "./UsersRepository.ts";

describe("UsersRepository", () => {
  let db: DenoDbContext;
  let repository: UsersRepository;
  const { before, after } = makeDenoDbBeforeAfter();

  let createdUserId: number;

  beforeAll(async () => {
    db = await before();
    repository = new UsersRepository(db.connection);
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
