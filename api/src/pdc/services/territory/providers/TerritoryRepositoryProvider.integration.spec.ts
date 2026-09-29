import { assertEquals } from "dep:assert";
import { afterAll, beforeAll, describe, it } from "dep:testing-bdd";
import { KernelInterfaceResolver } from "@/ilos/common/index.ts";
import { LegacyPostgresConnection } from "@/ilos/connection-postgres/index.ts";
import sql from "@/lib/pg/sql.ts";
import { DenoDbContext, makeDenoDbBeforeAfter } from "@/pdc/providers/test/index.ts";
import { TerritoryRepositoryProvider } from "./TerritoryRepositoryProvider.ts";

class TestKernel extends KernelInterfaceResolver {}

describe("TerritoryRepositoryProvider", () => {
  let db: DenoDbContext;
  let legacy: LegacyPostgresConnection;
  let repository: TerritoryRepositoryProvider;
  const { before, after } = makeDenoDbBeforeAfter();

  beforeAll(async () => {
    db = await before();
    legacy = new LegacyPostgresConnection({ connectionString: db.db.currentConnectionString });
    await legacy.up();
    repository = new TerritoryRepositoryProvider(legacy, new TestKernel());
  });

  afterAll(async () => {
    await legacy.down();
    await after(db);
  });

  it("list includes territories without company", async () => {
    const rows = await db.connection.query<{ _id: number }>(sql`
      INSERT INTO territory.territory_group (name, company_id)
      VALUES ('SCoT sans entreprise', NULL)
      RETURNING _id
    `);

    const result = await repository.list({ search: "scot sans entreprise" });

    assertEquals(result.data, [{ _id: rows[0]._id, name: "SCoT sans entreprise", siret: null }] as never);
    assertEquals(result.meta.pagination.total, 1);
  });
});
