import { assertEquals } from "dep:assert";
import { afterAll, beforeAll, describe, it } from "dep:testing-bdd";

import { DenoDbContext, makeDenoDbBeforeAfter } from "@/pdc/providers/test/dbMacro.ts";

import { LocationRepositoryProvider } from "./LocationRepositoryProvider.ts";

describe("LocationRepositoryProvider", () => {
  let db: DenoDbContext;
  let repository: LocationRepositoryProvider;
  const { before, after } = makeDenoDbBeforeAfter();

  beforeAll(async () => {
    db = await before();
    repository = new LocationRepositoryProvider(db.connection);
  });

  afterAll(async () => {
    await after(db);
  });

  it("getLocation returns an empty hex bucket list when no row matches", async () => {
    const result = await repository.getLocation({
      year: 2099,
      type: "com",
      code: "00000",
      zoom: 8,
    });
    assertEquals(result, []);
  });
});
