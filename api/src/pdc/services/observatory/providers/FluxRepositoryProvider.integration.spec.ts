import { assertEquals } from "dep:assert";
import { afterAll, beforeAll, describe, it } from "dep:testing-bdd";

import { DenoDbContext, makeDenoDbBeforeAfter } from "@/pdc/providers/test/dbMacro.ts";

import { FluxRepositoryProvider } from "./FluxRepositoryProvider.ts";

describe("FluxRepositoryProvider", () => {
  let db: DenoDbContext;
  let repository: FluxRepositoryProvider;
  const { before, after } = makeDenoDbBeforeAfter();

  beforeAll(async () => {
    db = await before();
    repository = new FluxRepositoryProvider(db.connection);
  });

  afterAll(async () => {
    await after(db);
  });

  it("getFlux returns an empty list when no row matches", async () => {
    const result = await repository.getFlux({
      year: 2099,
      type: "com",
      code: "00000",
      observe: "com",
    });
    assertEquals(result, []);
  });

  it("getEvolFlux returns an empty list when no row matches", async () => {
    const result = await repository.getEvolFlux({
      type: "com",
      code: "00000",
      indic: "journeys",
    });
    assertEquals(result, []);
  });

  it("getBestFlux returns an empty list when no row matches", async () => {
    const result = await repository.getBestFlux({
      year: 2099,
      type: "com",
      code: "00000",
      limit: 10,
    });
    assertEquals(result, []);
  });
});
