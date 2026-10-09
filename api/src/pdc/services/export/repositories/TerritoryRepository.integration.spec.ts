import { assertEquals } from "dep:assert";
import { afterAll, beforeAll, describe, it } from "dep:testing-bdd";
import { DenoDbContext, makeDenoDbBeforeAfter } from "@/pdc/providers/test/index.ts";
import { PerimeterRepositoryProvider } from "@/pdc/services/territory/providers/PerimeterRepositoryProvider.ts";
import { TerritoryRepository } from "./TerritoryRepository.ts";

// seeded territory 1 (aom:217500016) has selectors but no perimeter version
const SEEDED_TERRITORY = 1;

describe("TerritoryRepository: custom territories", () => {
  let repository: TerritoryRepository;
  let db: DenoDbContext;
  let custom_id: number;
  const { before, after } = makeDenoDbBeforeAfter();

  beforeAll(async () => {
    db = await before();
    repository = new TerritoryRepository(db.connection);
    custom_id = await new PerimeterRepositoryProvider(db.connection).createTerritory("Custom export", undefined, {
      arr: ["91477", "91471"],
      valid_from: new Date("2026-01-01T00:00:00Z"),
      valid_to: null,
    });
  });

  afterAll(async () => {
    await after(db);
  });

  it("returns the arr of a custom territory over the period", async () => {
    const arr = await repository.getTerritoryPerimeterArr(
      custom_id,
      new Date("2026-02-01T00:00:00Z"),
      new Date("2026-03-01T00:00:00Z"),
    );
    assertEquals(arr, ["91471", "91477"]);
  });

  it("returns null for a territory without perimeter version", async () => {
    const arr = await repository.getTerritoryPerimeterArr(
      SEEDED_TERRITORY,
      new Date("2026-02-01T00:00:00Z"),
      new Date("2026-03-01T00:00:00Z"),
    );
    assertEquals(arr, null);
  });

  it("names territory groups", async () => {
    assertEquals(await repository.getTerritoryGroupNames([custom_id, SEEDED_TERRITORY]), [
      "Custom export",
      "Ile-De-France-Mobilité",
    ]);
  });
});
