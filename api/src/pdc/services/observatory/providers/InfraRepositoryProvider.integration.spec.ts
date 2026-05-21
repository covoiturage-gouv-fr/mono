import { assertEquals } from "dep:assert";
import { afterAll, beforeAll, describe, it } from "dep:testing-bdd";

import { DenoDbContext, makeDenoDbBeforeAfter } from "@/pdc/providers/test/dbMacro.ts";

import { InfraRepositoryProvider } from "./InfraRepositoryProvider.ts";

describe("InfraRepositoryProvider", () => {
  let db: DenoDbContext;
  let repository: InfraRepositoryProvider;
  const { before, after } = makeDenoDbBeforeAfter();

  beforeAll(async () => {
    db = await before();
    repository = new InfraRepositoryProvider(db.connection);
  });

  afterAll(async () => {
    await after(db);
  });

  it("getAiresCovoiturage returns an empty list when no aire is open", async () => {
    const result = await repository.getAiresCovoiturage({});
    assertEquals(result, []);
  });

  it("getAiresCovoiturage scopes the result by territory code via the perimeter table", async () => {
    const result = await repository.getAiresCovoiturage({ type: "com", code: "00000" });
    assertEquals(result, []);
  });
});
