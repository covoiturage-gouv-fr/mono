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

  it("resolves selectors to arr and reports unknown codes", async () => {
    const result = await repository.resolve({
      epci: ["200056232"],
      arr: ["69381"],
      aom: ["123456789"],
    });

    assertEquals(result.arr, ["69381", "91377", "91471", "91477"]);
    assertEquals(result.unknown, ["aom:123456789"]);
  });

  it("resolves a com selector to its arrondissements", async () => {
    const result = await repository.resolve({ com: ["69123"] });
    assertEquals(result.arr, ["69381", "69382", "69383", "69384", "69385", "69386", "69387", "69388", "69389"]);
  });

  it("keeps territory.get_com_by_territory_id results", async () => {
    const rows = await db.connection.query<{ com: string }>(sql`
      SELECT com FROM territory.get_com_by_territory_id(1, 2021::smallint) ORDER BY com
    `);
    assertEquals(rows.map((r) => r.com), ["91377", "91471", "91477"]);
  });

  it("lists code changes only", async () => {
    await db.connection.query(sql`
      INSERT INTO geo.com_evolution (year, mod, old_com, new_com, l_mod) VALUES
        (2024, 32, '91471', '91999', 'fusion'),
        (2024, 10, '91477', '91477', 'changement de nom')
    `);
    assertEquals(await repository.findEvolutions(), [{ old_com: "91471", new_com: "91999" }]);
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
