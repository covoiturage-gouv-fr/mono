import { assert, assertEquals, assertNotEquals } from "dep:assert";
import { afterAll, beforeAll, describe, it } from "dep:testing-bdd";
import sql, { raw } from "../../../../lib/pg/sql.ts";
import { DenoDbContext, makeDenoDbBeforeAfter } from "../../../providers/test/index.ts";

import { PolicyStatusEnum } from "../contracts/common/interfaces/PolicyInterface.ts";
import { SerializedPolicyInterface } from "../interfaces/index.ts";
import { PolicyRepositoryProvider } from "./PolicyRepositoryProvider.ts";

describe("PolicyRepositoryProvider", () => {
  let repository: PolicyRepositoryProvider;
  let territory_id: number;
  let policy: SerializedPolicyInterface;
  let db: DenoDbContext;

  function makePolicy(
    data: Partial<SerializedPolicyInterface> = {},
  ): Omit<SerializedPolicyInterface, "_id"> {
    const start_date = new Date();
    start_date.setDate(-7);
    const end_date = new Date();
    end_date.setDate(end_date.getDate() + 7);

    return {
      territory_id: 1,
      territory_selector: {},
      name: "policy",
      start_date,
      end_date,
      tz: "Europe/Paris",
      status: PolicyStatusEnum.DRAFT,
      handler: "Idfm",
      incentive_sum: 5000,
      max_amount: 10_000_000_00,
      ...data,
    };
  }
  const { before, after } = makeDenoDbBeforeAfter();

  beforeAll(async () => {
    territory_id = 1;
    db = await before();
    repository = new PolicyRepositoryProvider(
      db.connection,
    );
    policy = { ...makePolicy(), _id: 0 };
    territory_id = 1;
  });

  afterAll(async () => {
    await after(db);
  });

  it("Should create policy", async () => {
    const { _id, ...policyData } = policy;
    const policyL = await repository.create(policyData);

    const rows = await db.connection.query<{ name: string; status: string }>(sql`
      SELECT * FROM ${raw(repository.table)} WHERE _id = ${policyL._id}
    `);

    assertEquals(rows.length, 1);
    assertEquals(rows[0].name, policyData.name);
    assertEquals(rows[0].status, "draft");
    policy._id = policyL._id;
  });

  it("Should find policy", async () => {
    const policyL = await repository.find(policy._id);
    assertEquals(policyL?.name, policy.name);
    assertEquals(policyL?.status, policy.status);
  });

  it("Should find policy by territory", async () => {
    const policyL = await repository.find(
      policy._id,
      territory_id,
    );
    assertEquals(policyL?.name, policy.name);
    assertEquals(policyL?.status, policy.status);
  });

  it("Should find policy where territory", async () => {
    const policies = await repository.findWhere({
      territory_id: territory_id,
    });
    assert(Array.isArray(policies));
    assertEquals(policies.length, 1);
    const policyL = policies.pop();
    assertEquals(policyL?.name, policy.name);
    assertEquals(policyL?.status, policy.status);
  });

  it("Should not find policy by territory", async () => {
    const policyL = await repository.find(policy._id, 2);
    assertEquals(policyL, undefined);
  });

  it("Should patch policy", async () => {
    const name = "Awesome policy";
    const policyL = await repository.patch({
      ...policy,
      name,
    });

    const rows = await db.connection.query<{ name: string }>(sql`
      SELECT * FROM ${raw(repository.table)} WHERE _id = ${policyL._id}
    `);

    assertNotEquals(policy.name, policyL.name);
    assertEquals(policyL.name, name);
    assertEquals(rows.length, 1);
    assertEquals(rows[0].name, name);
    policy.name = name;
  });

  it("Should delete policy", async () => {
    await repository.delete(policy._id);

    const rows = await db.connection.query<{ deleted_at: Date | null }>(sql`
      SELECT deleted_at FROM ${raw(repository.table)} WHERE _id = ${policy._id}
    `);

    assertEquals(rows.length, 1);
    assertNotEquals(rows[0].deleted_at, null);
  });
});
