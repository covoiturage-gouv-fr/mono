import { assertEquals } from "dep:assert";
import { afterAll, beforeAll, describe, it } from "dep:testing-bdd";
import { env_or_fail } from "@/lib/env/index.ts";
import { LegacyMigrator } from "./LegacyMigrator.ts";

describe("seed", () => {
  const mig = new LegacyMigrator(env_or_fail("APP_POSTGRES_URL"));
  beforeAll(async () => {
    await mig.create();
    await mig.up();
    await mig.migrate({ flash: false, verbose: false });
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
});
