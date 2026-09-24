import { assertEquals, assertThrows } from "dep:assert";
import { describe, it } from "dep:testing-bdd";
import { PolicyTerritoryInterface, TerritoryCodeEnum } from "../interfaces/index.ts";
import { applyOperation, diffArr, findVersionAt, isCovered, parseTerritoryCodes } from "./territories.ts";

function version(
  v: number,
  from: string,
  to: string | null,
  arr: string[] = [],
): PolicyTerritoryInterface {
  return { version: v, arr, valid_from: new Date(from), valid_to: to ? new Date(to) : null };
}

describe("parseTerritoryCodes", () => {
  it("parses type:code tokens", () => {
    assertEquals(parseTerritoryCodes(["aom:241700434", "com:17300"]), [
      { type: TerritoryCodeEnum.Mobility, code: "241700434" },
      { type: TerritoryCodeEnum.City, code: "17300" },
    ]);
  });

  it("splits comma separated tokens and deduplicates", () => {
    assertEquals(parseTerritoryCodes(["com:17300,com:17306", "com:17300"]), [
      { type: TerritoryCodeEnum.City, code: "17300" },
      { type: TerritoryCodeEnum.City, code: "17306" },
    ]);
  });

  it("accepts corsican codes, overseas dep, network and country", () => {
    assertEquals(parseTerritoryCodes(["com:2A004", "dep:2B", "dep:971", "reseau:232", "country:XXXXX"]), [
      { type: TerritoryCodeEnum.City, code: "2A004" },
      { type: TerritoryCodeEnum.District, code: "2B" },
      { type: TerritoryCodeEnum.District, code: "971" },
      { type: TerritoryCodeEnum.Network, code: "232" },
      { type: TerritoryCodeEnum.Country, code: "XXXXX" },
    ]);
  });

  it("rejects unknown type", () => {
    assertThrows(() => parseTerritoryCodes(["foo:17300"]), Error, "foo:17300");
  });

  it("rejects missing type", () => {
    assertThrows(() => parseTerritoryCodes(["17300"]), Error, "17300");
  });

  it("rejects a code not matching its type", () => {
    assertThrows(() => parseTerritoryCodes(["com:241700434"]), Error, "com:241700434");
    assertThrows(() => parseTerritoryCodes(["aom:17300"]), Error, "aom:17300");
  });

  it("rejects empty input", () => {
    assertThrows(() => parseTerritoryCodes([]), Error);
  });
});

describe("findVersionAt", () => {
  const versions = [
    version(1, "2026-01-01", null, ["1"]),
    version(2, "2026-07-01", null, ["2"]),
    version(3, "2026-03-01", "2026-04-01", ["3"]),
  ];

  it("picks the highest version covering the date", () => {
    assertEquals(findVersionAt(versions, new Date("2026-02-01"))?.version, 1);
    assertEquals(findVersionAt(versions, new Date("2026-03-15"))?.version, 3);
    assertEquals(findVersionAt(versions, new Date("2026-08-01"))?.version, 2);
  });

  it("treats valid_to as exclusive", () => {
    assertEquals(findVersionAt(versions, new Date("2026-04-01"))?.version, 1);
  });

  it("returns undefined when no version covers the date", () => {
    assertEquals(findVersionAt(versions, new Date("2025-12-31")), undefined);
    assertEquals(findVersionAt([], new Date("2026-01-01")), undefined);
  });
});

describe("isCovered", () => {
  it("is true when contiguous versions cover the range", () => {
    const versions = [
      version(1, "2026-01-01", "2026-03-01"),
      version(2, "2026-03-01", null),
    ];
    assertEquals(isCovered(versions, new Date("2026-01-01"), new Date("2027-01-01")), true);
  });

  it("is false when there is a gap", () => {
    const versions = [
      version(1, "2026-01-01", "2026-03-01"),
      version(2, "2026-04-01", null),
    ];
    assertEquals(isCovered(versions, new Date("2026-01-01"), new Date("2026-05-01")), false);
  });

  it("is false when the range starts before the first version", () => {
    const versions = [version(1, "2026-02-01", null)];
    assertEquals(isCovered(versions, new Date("2026-01-01"), new Date("2026-05-01")), false);
  });

  it("ignores version order", () => {
    const versions = [
      version(2, "2026-03-01", null),
      version(1, "2026-01-01", "2026-03-01"),
    ];
    assertEquals(isCovered(versions, new Date("2026-01-01"), new Date("2026-05-01")), true);
  });

  it("is false without versions", () => {
    assertEquals(isCovered([], new Date("2026-01-01"), new Date("2026-05-01")), false);
  });
});

describe("applyOperation", () => {
  it("add is a sorted union", () => {
    assertEquals(applyOperation("add", ["17300", "17001"], ["17306", "17300"]), ["17001", "17300", "17306"]);
  });

  it("remove is a difference", () => {
    assertEquals(applyOperation("remove", ["17001", "17300", "17306"], ["17300", "99999"]), ["17001", "17306"]);
  });

  it("set replaces", () => {
    assertEquals(applyOperation("set", ["17001"], ["17306", "17300"]), ["17300", "17306"]);
  });
});

describe("diffArr", () => {
  it("returns added and removed codes", () => {
    assertEquals(diffArr(["1", "2", "3"], ["2", "3", "4", "5"]), { added: ["4", "5"], removed: ["1"] });
  });
});
