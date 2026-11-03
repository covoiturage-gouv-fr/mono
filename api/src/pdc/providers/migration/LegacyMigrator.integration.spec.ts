import { assertEquals } from "dep:assert";
import { afterAll, beforeAll, describe, it } from "dep:testing-bdd";
import { env_or_fail } from "@/lib/env/index.ts";
import { LegacyMigrator } from "./LegacyMigrator.ts";
import { adminIleDeFrance, adminMaxiCovoit } from "./seeds/users.ts";

describe("seed", () => {
  const mig = new LegacyMigrator(env_or_fail("APP_POSTGRES_URL"));
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
    const result = await mig.testConn.getClient().query({
      text: "SELECT count(*) FROM geo.perimeters",
    });
    assertEquals(result.rows[0].count, "17");
  });

  it("should restore user scopes when seeding twice", async () => {
    await mig.testConn.getClient().query({ text: "DELETE FROM auth.user_scopes" });

    await mig.seedUser(adminMaxiCovoit);
    await mig.seedUser(adminIleDeFrance);

    const result = await mig.testConn.getClient().query({
      text: `
        SELECT u.email, s.operator_id, s.territory_id
        FROM auth.user_scopes s
        JOIN auth.users u ON u._id = s.user_id
        WHERE u.email IN ($1, $2)
        ORDER BY u.email
      `,
      values: [adminMaxiCovoit.email, adminIleDeFrance.email],
    });

    assertEquals(result.rows, [
      { email: adminMaxiCovoit.email, operator_id: 1, territory_id: null },
      { email: adminIleDeFrance.email, operator_id: null, territory_id: 1 },
    ]);
  });
});
