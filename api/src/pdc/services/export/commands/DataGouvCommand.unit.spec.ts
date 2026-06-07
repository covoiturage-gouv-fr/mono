import { assertThrows } from "dep:assert";
import { describe, it } from "dep:testing-bdd";
import { DataGouvStatsType } from "../repositories/queries/datagouvStatsQuery.ts";
import { assertDatagouvNotEmpty } from "./DataGouvCommand.ts";

describe("assertDatagouvNotEmpty", () => {
  function fakeStats(overrides: Partial<DataGouvStatsType> = {}): DataGouvStatsType {
    return {
      start_at: new Date("2025-05-01"),
      end_at: new Date("2025-06-01"),
      count_total: 100,
      count_exposed: 100,
      count_removed: 0,
      count_removed_start: 0,
      count_removed_end: 0,
      count_removed_both: 0,
      ...overrides,
    };
  }

  it("throws when no rows are exposed (upstream data not ready)", () => {
    assertThrows(() => assertDatagouvNotEmpty(fakeStats({ count_total: 0, count_exposed: 0 }), "f.csv"));
  });

  it("throws when total exists but everything is filtered out", () => {
    assertThrows(() => assertDatagouvNotEmpty(fakeStats({ count_total: 50, count_exposed: 0 }), "f.csv"));
  });

  it("throws when count comes back as a string '0' (pg numeric)", () => {
    assertThrows(() => assertDatagouvNotEmpty(fakeStats({ count_exposed: "0" as unknown as number }), "f.csv"));
  });

  it("does not throw when rows are exposed", () => {
    assertDatagouvNotEmpty(fakeStats({ count_exposed: 1 }), "f.csv");
  });
});
