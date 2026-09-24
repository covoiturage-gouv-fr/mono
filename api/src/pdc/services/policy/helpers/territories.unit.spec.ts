import { assertEquals, assertThrows } from "dep:assert";
import { describe, it } from "dep:testing-bdd";
import { PolicyTerritoryInterface } from "../interfaces/index.ts";
import { applyOperation, diffArr, findVersionAt, isCovered, parseTerritoryCodes, successors } from "./territories.ts";

function version(
  v: number,
  from: string,
  to: string | null,
  arr: string[] = [],
): PolicyTerritoryInterface {
  return { version: v, arr, valid_from: new Date(from), valid_to: to ? new Date(to) : null };
}

describe("parseTerritoryCodes", () => {
  it("groups type:code tokens into selectors", () => {
    assertEquals(parseTerritoryCodes(["aom:241700434", "com:17300", "com:17306"]), {
      aom: ["241700434"],
      com: ["17300", "17306"],
    });
  });

  it("splits comma separated tokens and deduplicates", () => {
    assertEquals(parseTerritoryCodes(["com:17300,com:17306", "com:17300"]), { com: ["17300", "17306"] });
  });

  it("normalises case", () => {
    assertEquals(parseTerritoryCodes(["COM:2a004", "Dep:2b"]), { com: ["2A004"], dep: ["2B"] });
  });

  it("accepts every campaign scale", () => {
    assertEquals(
      parseTerritoryCodes(["arr:69381", "com:17300", "epci:200041762", "aom:241700434", "dep:971", "reg:75"]),
      {
        arr: ["69381"],
        com: ["17300"],
        epci: ["200041762"],
        aom: ["241700434"],
        dep: ["971"],
        reg: ["75"],
      },
    );
  });

  it("rejects scales without campaigns", () => {
    assertThrows(() => parseTerritoryCodes(["country:XXXXX"]), Error, "country:XXXXX");
    assertThrows(() => parseTerritoryCodes(["reseau:232"]), Error, "reseau:232");
  });

  it("rejects unknown or missing type", () => {
    assertThrows(() => parseTerritoryCodes(["foo:17300"]), Error, "foo:17300");
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

describe("successors", () => {
  it("follows merges and splits", () => {
    const evolutions = [
      { old_com: "01001", new_com: "01100" },
      { old_com: "01002", new_com: "01100" },
      { old_com: "02001", new_com: "02100" },
      { old_com: "02001", new_com: "02200" },
    ];
    assertEquals(successors(["01001", "02001", "03001"], evolutions), ["01100", "02100", "02200"]);
  });

  it("follows chains and ignores codes already present", () => {
    const evolutions = [
      { old_com: "01001", new_com: "01100" },
      { old_com: "01100", new_com: "01200" },
      { old_com: "01200", new_com: "01200" },
    ];
    assertEquals(successors(["01001", "01100"], evolutions), ["01200"]);
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
