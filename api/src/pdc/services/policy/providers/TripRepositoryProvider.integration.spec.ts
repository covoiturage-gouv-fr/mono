import { assertEquals } from "dep:assert";
import { afterAll, beforeAll, describe, it } from "dep:testing-bdd";
import { DenoDbContext, makeDenoDbBeforeAfter } from "@/pdc/providers/test/dbMacro.ts";
import { CarpoolInterface } from "../interfaces/index.ts";
import { TripRepositoryProvider } from "./TripRepositoryProvider.ts";

describe("TripRepositoryProvider", () => {
  let db: DenoDbContext;
  let repository: TripRepositoryProvider;
  const { before, after } = makeDenoDbBeforeAfter();

  beforeAll(async () => {
    db = await before();
    repository = new TripRepositoryProvider(db.connection);
  });

  afterAll(async () => {
    await after(db);
  });

  it("findTripByGeo compiles the operator incentives subquery against the real schema", async () => {
    const batches: CarpoolInterface[][] = [];
    for await (
      const rows of repository.findTripByGeo(
        ["99999"],
        new Date("2099-01-01T00:00:00Z"),
        new Date("2099-02-01T00:00:00Z"),
      )
    ) {
      batches.push(rows);
    }
    assertEquals(batches, []);
  });
});
