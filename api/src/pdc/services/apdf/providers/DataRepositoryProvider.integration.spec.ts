import { assertEquals } from "dep:assert";
import { afterAll, beforeAll, describe, it } from "dep:testing-bdd";

import { DenoDbContext, makeDenoDbBeforeAfter } from "@/pdc/providers/test/dbMacro.ts";

import { APDFTripInterface } from "../interfaces/APDFTripInterface.ts";
import { DataRepositoryProvider } from "./DataRepositoryProvider.ts";

describe("DataRepositoryProvider", () => {
  let db: DenoDbContext;
  let repository: DataRepositoryProvider;
  const { before, after } = makeDenoDbBeforeAfter();

  const params = {
    campaign_id: 9_999,
    operator_id: 9_999,
    start_date: new Date("2099-01-01T00:00:00Z"),
    end_date: new Date("2099-02-01T00:00:00Z"),
  };

  beforeAll(async () => {
    db = await before();
    repository = new DataRepositoryProvider(db.connection);
  });

  afterAll(async () => {
    await after(db);
  });

  it("getPolicyActiveOperators returns an empty list when no carpools match the campaign window", async () => {
    const result = await repository.getPolicyActiveOperators(
      params.campaign_id,
      params.start_date,
      params.end_date,
    );
    assertEquals(result, []);
  });

  it("getPolicyStats returns a zero total_count when no carpools match", async () => {
    const result = await repository.getPolicyStats(params, []);
    // count(*) over empty returns 0; sum() over empty returns null
    // (pre-existing behaviour preserved by the migration).
    assertEquals(result.total_count, 0);
    assertEquals(result.subsidized_count, 0);
    assertEquals(result.slices, []);
  });

  it("getPolicyStats compiles the slice filter clauses without crashing", async () => {
    const slices = [
      { start: 0, end: 5_000 },
      { start: 5_000, end: 15_000 },
      { start: 15_000 },
    ];
    const result = await repository.getPolicyStats(params, slices);
    assertEquals(result.total_count, 0);
    assertEquals(result.subsidized_count, 0);
  });

  it("getPolicyCursor opens a cursor that yields no batches when no carpools match", async () => {
    await using cursor = await repository.getPolicyCursor(params);
    const batches: APDFTripInterface[][] = [];
    for await (const rows of cursor.read(50)) {
      batches.push(rows);
    }
    assertEquals(batches, []);
  });

  // GEN-643 : valide que la jointure latérale du montant déclaré (appariement par
  // SIREN sur carpool_v2.operator_incentives) est un SQL valide contre le schéma réel.
  it("getPolicyCursor supports the declared-incentive lateral join (SIREN) without crashing", async () => {
    await using cursor = await repository.getPolicyCursor({ ...params, declared_siren: "287500078" });
    const batches: APDFTripInterface[][] = [];
    for await (const rows of cursor.read(50)) {
      batches.push(rows);
    }
    assertEquals(batches, []);
  });
});
