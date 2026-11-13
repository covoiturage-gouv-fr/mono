import { assertEquals } from "dep:assert";
import { afterAll, beforeAll, describe, it } from "dep:testing-bdd";
import { env_or_fail } from "../../../lib/env/index.ts";
import sql from "../../../lib/pg/sql.ts";
import { DenoMigrator } from "./DenoMigrator.ts";
import { adminIleDeFrance, adminMaxiCovoit } from "./seeds/users.ts";

describe("seed", () => {
  const mig = new DenoMigrator(env_or_fail("APP_POSTGRES_URL"));
  beforeAll(async () => {
    await mig.create();
    await mig.up();
    await mig.migrate({ verbose: false });
    await mig.seed();
  });

  afterAll(async () => {
    await mig.drop();
    await mig.down();
  });

  it("should seed territories", async () => {
    const rows = await mig.testConn.query<{ count: number }>(sql`SELECT count(*) FROM geo.perimeters`);
    assertEquals(rows[0].count, 17);
  });

  it("should restore user scopes when seeding twice", async () => {
    await mig.testConn.query(sql`DELETE FROM auth.user_scopes`);

    await mig.seedUser(adminMaxiCovoit);
    await mig.seedUser(adminIleDeFrance);

    const rows = await mig.testConn.query<{ email: string; operator_id: number | null; territory_id: number | null }>(
      sql`
        SELECT u.email, s.operator_id, s.territory_id
        FROM auth.user_scopes s
        JOIN auth.users u ON u._id = s.user_id
        WHERE u.email IN (${adminMaxiCovoit.email}, ${adminIleDeFrance.email})
        ORDER BY u.email
      `,
    );

    assertEquals(rows, [
      { email: adminMaxiCovoit.email, operator_id: 1, territory_id: null },
      { email: adminIleDeFrance.email, operator_id: null, territory_id: 1 },
    ]);
  });
});
