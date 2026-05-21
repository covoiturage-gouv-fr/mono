import { assertEquals } from "dep:assert";
import { afterAll, beforeAll, describe, it } from "dep:testing-bdd";

import { DenoDbContext, makeDenoDbBeforeAfter } from "@/pdc/providers/test/dbMacro.ts";

import { LastRecordRepositoryProvider } from "./LastRecordRepositoryProvider.ts";

describe("LastRecordRepositoryProvider", () => {
  let db: DenoDbContext;
  let repository: LastRecordRepositoryProvider;
  const { before, after } = makeDenoDbBeforeAfter();

  beforeAll(async () => {
    db = await before();
    repository = new LastRecordRepositoryProvider(db.connection);
  });

  afterAll(async () => {
    await after(db);
  });

  it("getLastRecord returns null when no rows match the territory", async () => {
    const result = await repository.getLastRecord({ type: "com", code: "00000" });
    assertEquals(result, null);
  });

  it("getLastRecord accepts an optional maxYear / maxMonth ceiling without crashing", async () => {
    const result = await repository.getLastRecord({ type: "com", code: "00000" }, 2099, 12);
    assertEquals(result, null);
  });
});
