import { assertEquals } from "dep:assert";
import { afterAll, beforeAll, describe, it } from "dep:testing-bdd";
import { LegacyPostgresConnection } from "@/ilos/connection-postgres/index.ts";
import sql from "@/lib/pg/sql.ts";
import { DenoDbContext, makeDenoDbBeforeAfter } from "@/pdc/providers/test/index.ts";
import { TerritoryRepositoryProvider } from "./TerritoryRepositoryProvider.ts";

describe("TerritoryRepositoryProvider", () => {
  let db: DenoDbContext;
  let legacy: LegacyPostgresConnection;
  let repository: TerritoryRepositoryProvider;
  const { before, after } = makeDenoDbBeforeAfter();

  beforeAll(async () => {
    db = await before();
    legacy = new LegacyPostgresConnection({ connectionString: db.db.currentConnectionString });
    await legacy.up();
    repository = new TerritoryRepositoryProvider(legacy);
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

  it("list search ignores case and ranks prefix matches first", async () => {
    await db.connection.query(sql`
      INSERT INTO territory.territory_group (name, company_id)
      VALUES ('Zone Métropole Test', NULL), ('Métropole Test Accent', NULL)
    `);

    const result = await repository.list({ search: "MÉTROPOLE TEST" });

    assertEquals(
      result.data.map((t) => t.name),
      ["Métropole Test Accent", "Zone Métropole Test"],
    );
    assertEquals(result.meta.pagination.total, 2);
  });

  it("list honours limit while total counts every match", async () => {
    const result = await repository.list({ search: "métropole test", limit: 1 });

    assertEquals(result.data.length, 1);
    assertEquals(result.meta.pagination.limit, 1);
    assertEquals(result.meta.pagination.total, 2);
  });

  // La modale utilisateur ne charge plus de liste bornée à 200 : la recherche doit atteindre tout le catalogue.
  describe("list search over a large catalogue", () => {
    beforeAll(async () => {
      await db.connection.query(sql`
        INSERT INTO territory.territory_group (name, company_id)
        SELECT 'Catalogue ' || lpad(n::text, 4, '0'), NULL FROM generate_series(1, 250) AS n
      `);
      await db.connection.query(sql`
        INSERT INTO territory.territory_group (name, company_id) VALUES ('Zzz périmètre sur mesure', NULL)
      `);
    });

    it("finds a territory beyond the 200th by name", async () => {
      const result = await repository.list({ search: "catalogue 0250", limit: 20 });

      assertEquals(result.data.map((t) => t.name), ["Catalogue 0250"]);
    });

    it("finds a custom territory sorted last alphabetically", async () => {
      const result = await repository.list({ search: "sur mesure", limit: 20 });

      assertEquals(result.data.map((t) => t.name), ["Zzz périmètre sur mesure"]);
    });

    it("pages through every match with offset", async () => {
      const last = await repository.list({ search: "catalogue", limit: 200, offset: 200 });

      assertEquals(last.meta.pagination.total, 250);
      assertEquals(last.data.length, 50);
    });
  });
});
