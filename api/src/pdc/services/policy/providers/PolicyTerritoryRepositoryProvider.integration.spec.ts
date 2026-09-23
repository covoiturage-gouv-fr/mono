import { assertEquals, assertRejects } from "dep:assert";
import { afterAll, beforeAll, describe, it } from "dep:testing-bdd";
import sql from "@/lib/pg/sql.ts";
import { DenoDbContext, makeDenoDbBeforeAfter } from "@/pdc/providers/test/index.ts";
import { PolicyTerritoryRepositoryProvider } from "./PolicyTerritoryRepositoryProvider.ts";

describe("PolicyTerritoryRepositoryProvider", () => {
  let repository: PolicyTerritoryRepositoryProvider;
  let db: DenoDbContext;
  let policy_id: number;
  const { before, after } = makeDenoDbBeforeAfter();

  beforeAll(async () => {
    db = await before();
    repository = new PolicyTerritoryRepositoryProvider(db.connection);
    const rows = await db.connection.query<{ _id: number }>(sql`
      INSERT INTO policy.policies (name, status, handler, start_date, end_date)
      VALUES ('policy_territories', 'draft', 'Idfm', '2026-01-01', '2027-01-01')
      RETURNING _id
    `);
    policy_id = rows[0]._id;
  });

  afterAll(async () => {
    await after(db);
  });

  it("resolves mixed codes to arr, clamping the year to the latest millesime", async () => {
    const result = await repository.resolve([
      { type: "epci", code: "200056232" },
      { type: "arr", code: "69381" },
      { type: "aom", code: "123456789" },
    ], 2026);

    assertEquals(result.arr, ["69381", "91377", "91471", "91477"]);
    assertEquals(result.unknown, [{ type: "aom", code: "123456789" }]);
  });

  it("describes arr with their label", async () => {
    const rows = await repository.describe(["91471", "91477"]);
    assertEquals(rows.map((r) => [r.arr, r.label]), [["91471", "Orsay"], ["91477", "Palaiseau"]]);
  });

  it("creates incremental versions", async () => {
    const v1 = await repository.create(policy_id, {
      arr: ["91471"],
      valid_from: new Date("2026-01-01T00:00:00Z"),
      valid_to: null,
    });
    const v2 = await repository.create(policy_id, {
      arr: ["91471", "91477"],
      valid_from: new Date("2026-07-01T00:00:00Z"),
      valid_to: new Date("2026-09-01T00:00:00Z"),
    });

    assertEquals([v1.version, v2.version], [1, 2]);

    const versions = await repository.findByPolicy(policy_id);
    assertEquals(versions.map((v) => [v.version, v.arr, v.valid_to]), [
      [1, ["91471"], null],
      [2, ["91471", "91477"], new Date("2026-09-01T00:00:00Z")],
    ]);
  });

  it("rejects an empty perimeter", async () => {
    await assertRejects(() =>
      repository.create(policy_id, { arr: [], valid_from: new Date("2026-01-01"), valid_to: null })
    );
  });
});
