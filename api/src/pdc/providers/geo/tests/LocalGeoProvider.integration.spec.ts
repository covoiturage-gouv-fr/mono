import { assertEquals, assertRejects } from "dep:assert";
import { afterAll, beforeAll, describe, it } from "dep:testing-bdd";

import { NotFoundException } from "@/ilos/common/index.ts";
import { DenoDbContext, makeDenoDbBeforeAfter } from "@/pdc/providers/test/dbMacro.ts";

import { LocalGeoProvider } from "../providers/LocalGeoProvider.ts";

describe("LocalGeoProvider", () => {
  let db: DenoDbContext;
  let provider: LocalGeoProvider;
  const { before, after } = makeDenoDbBeforeAfter();

  beforeAll(async () => {
    db = await before();
    provider = new LocalGeoProvider(db.connection);
  });

  afterAll(async () => {
    await after(db);
  });

  it("positionToInsee returns the commune INSEE for a point inside a seeded polygon", async () => {
    const arr = await provider.positionToInsee({ lat: 48.721505, lon: 2.26188 });
    assertEquals(arr, "91377");
  });

  it("positionToInsee throws NotFoundException when no polygon nor closest-com fixture matches", async () => {
    await assertRejects(
      () => provider.positionToInsee({ lat: 0, lon: 0 }),
      NotFoundException,
    );
  });
});
