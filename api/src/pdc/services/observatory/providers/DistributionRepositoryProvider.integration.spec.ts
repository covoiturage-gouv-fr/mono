import { assertEquals } from "dep:assert";
import { afterAll, beforeAll, describe, it } from "dep:testing-bdd";

import { DenoDbContext, makeDenoDbBeforeAfter } from "@/pdc/providers/test/dbMacro.ts";

import { DistributionRepositoryProvider } from "./DistributionRepositoryProvider.ts";

describe("DistributionRepositoryProvider", () => {
  let db: DenoDbContext;
  let repository: DistributionRepositoryProvider;
  const { before, after } = makeDenoDbBeforeAfter();

  beforeAll(async () => {
    db = await before();
    repository = new DistributionRepositoryProvider(db.connection);
  });

  afterAll(async () => {
    await after(db);
  });

  it("getJourneysByHours returns an empty list when no row matches", async () => {
    const result = await repository.getJourneysByHours({
      year: 2099,
      type: "com",
      code: "00000",
    });
    assertEquals(result, []);
  });

  it("getJourneysByDistances returns an empty list when no row matches", async () => {
    const result = await repository.getJourneysByDistances({
      year: 2099,
      type: "com",
      code: "00000",
      direction: "both",
    });
    assertEquals(result, []);
  });
});
