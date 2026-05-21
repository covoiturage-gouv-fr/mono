import { assertEquals } from "dep:assert";
import { afterAll, beforeAll, describe, it } from "dep:testing-bdd";

import { DenoDbContext, makeDenoDbBeforeAfter } from "@/pdc/providers/test/dbMacro.ts";

import { OccupationRepositoryProvider } from "./OccupationRepositoryProvider.ts";

describe("OccupationRepositoryProvider", () => {
  let db: DenoDbContext;
  let repository: OccupationRepositoryProvider;
  const { before, after } = makeDenoDbBeforeAfter();

  beforeAll(async () => {
    db = await before();
    repository = new OccupationRepositoryProvider(db.connection);
  });

  afterAll(async () => {
    await after(db);
  });

  it("getOccupation returns an empty list when no row matches", async () => {
    const result = await repository.getOccupation({
      year: 2099,
      type: "com",
      code: "00000",
      observe: "com",
      direction: "both",
    });
    assertEquals(result, []);
  });

  it("getEvolOccupation returns an empty list when no row matches", async () => {
    const result = await repository.getEvolOccupation({
      type: "com",
      code: "00000",
      indic: "journeys",
    });
    assertEquals(result, []);
  });

  it("getBestTerritories returns an empty list when no row matches", async () => {
    const result = await repository.getBestTerritories({
      year: 2099,
      type: "com",
      code: "00000",
      observe: "com",
      limit: 10,
    });
    assertEquals(result, []);
  });
});
