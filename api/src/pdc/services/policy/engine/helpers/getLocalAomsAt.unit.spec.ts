import { assertEquals } from "dep:assert";
import { describe, it } from "dep:testing-bdd";
import { getLocalAomsAt, LocalAomEnds } from "./getLocalAomsAt.ts";

describe("getLocalAomsAt", () => {
  const all = ["A", "B", "C"];
  const ends: LocalAomEnds = [
    { siren: "B", date: new Date("2026-08-01T00:00:00+0200") },
    { siren: "C", date: new Date("2026-09-29T00:00:00+0200") },
  ];

  it("returns all AOM without date", () => assertEquals(getLocalAomsAt(all, ends), ["A", "B", "C"]));

  it("returns all AOM without any end", () => assertEquals(getLocalAomsAt(all, [], new Date("2030-01-01")), all));

  it("keeps all AOM before the first end", () =>
    assertEquals(getLocalAomsAt(all, ends, new Date("2026-07-31T23:59:59+0200")), ["A", "B", "C"]));

  it("end date is inclusive (Europe/Paris offset)", () => {
    assertEquals(getLocalAomsAt(all, ends, new Date("2026-07-31T21:59:59Z")), ["A", "B", "C"]);
    assertEquals(getLocalAomsAt(all, ends, new Date("2026-07-31T22:00:00Z")), ["A", "C"]);
  });

  it("cumulates ends, whatever the list order", () => {
    assertEquals(getLocalAomsAt(all, [...ends].reverse(), new Date("2026-09-29T00:00:00+0200")), ["A"]);
  });

  it("ignores unknown SIREN and does not mutate inputs", () => {
    const out = getLocalAomsAt(all, [{ siren: "Z", date: new Date(0) }], new Date());
    assertEquals(out, all);
    assertEquals(all, ["A", "B", "C"]);
  });
});
