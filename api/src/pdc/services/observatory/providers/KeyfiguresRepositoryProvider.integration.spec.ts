import { assertEquals } from "dep:assert";
import { afterAll, beforeAll, describe, it } from "dep:testing-bdd";

import { DenoDbContext, makeDenoDbBeforeAfter } from "@/pdc/providers/test/dbMacro.ts";

import { KeyfiguresRepositoryProvider } from "./KeyfiguresRepositoryProvider.ts";

describe("KeyfiguresRepositoryProvider", () => {
  let db: DenoDbContext;
  let repository: KeyfiguresRepositoryProvider;
  const { before, after } = makeDenoDbBeforeAfter();

  beforeAll(async () => {
    db = await before();
    repository = new KeyfiguresRepositoryProvider(db.connection);
  });

  afterAll(async () => {
    await after(db);
  });

  it("getKeyfigures returns an empty list when no row matches the territory window", async () => {
    const result = await repository.getKeyfigures({
      year: 2099,
      type: "com",
      code: "00000",
    });
    assertEquals(result, []);
  });

  it("getKeyfigures accepts the full set of period / direction filters", async () => {
    const result = await repository.getKeyfigures({
      year: 2099,
      type: "com",
      code: "00000",
      month: 12,
      direction: "both",
    });
    assertEquals(result, []);
  });
});
