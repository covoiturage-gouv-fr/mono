import { assertEquals } from "dep:assert";
import { afterAll, beforeAll, describe, it } from "dep:testing-bdd";

import { DenoDbContext, makeDenoDbBeforeAfter } from "@/pdc/providers/test/dbMacro.ts";

import { IncentiveRepositoryProvider } from "./IncentiveRepositoryProvider.ts";

describe("IncentiveRepositoryProvider", () => {
  let db: DenoDbContext;
  let repository: IncentiveRepositoryProvider;
  const { before, after } = makeDenoDbBeforeAfter();

  beforeAll(async () => {
    db = await before();
    repository = new IncentiveRepositoryProvider(db.connection);
  });

  afterAll(async () => {
    await after(db);
  });

  it("getIncentive returns an empty list when no row matches", async () => {
    const result = await repository.getIncentive({
      year: 2099,
      type: "com",
      code: "00000",
    });
    assertEquals(result, []);
  });

  it("getIncentive accepts the full set of period filters without crashing", async () => {
    const result = await repository.getIncentive({
      year: 2099,
      type: "com",
      code: "00000",
      month: 12,
      direction: "both",
    });
    assertEquals(result, []);
  });
});
