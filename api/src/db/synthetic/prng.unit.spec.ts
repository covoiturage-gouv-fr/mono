import { assert, assertEquals, assertNotEquals, assertThrows } from "dep:assert";
import { describe, it } from "dep:testing-bdd";
import { Prng } from "./prng.ts";

describe("synthetic / Prng", () => {
  const draw = (seed: string, n = 20) => {
    const prng = new Prng(seed);
    return Array.from({ length: n }, () => prng.next());
  };

  it("gives the same sequence for the same seed", () => {
    assertEquals(draw("seed-a"), draw("seed-a"));
  });

  it("gives another sequence for another seed", () => {
    assertNotEquals(draw("seed-a"), draw("seed-b"));
  });

  it("stays in [0, 1[", () => {
    assert(draw("range", 1000).every((n) => n >= 0 && n < 1));
  });

  it("draws integers within inclusive bounds", () => {
    const prng = new Prng("int");
    const values = new Set(Array.from({ length: 500 }, () => prng.int(2, 5)));
    assertEquals([...values].sort(), [2, 3, 4, 5]);
  });

  it("never picks a zero-weight key and refuses empty draws", () => {
    const prng = new Prng("weighted");
    const values = new Set(Array.from({ length: 500 }, () => prng.weighted({ a: 1, b: 0, c: 3 })));
    assertEquals([...values].sort(), ["a", "c"]);
    assertThrows(() => prng.weighted({ a: 0 }));
    assertThrows(() => prng.pick([]));
  });

  it("builds well-formed and reproducible uuids", () => {
    const uuid = new Prng("uuid").uuid();
    assert(/^[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/.test(uuid), uuid);
    assertEquals(uuid, new Prng("uuid").uuid());
    assertEquals(new Prng("hex").hex(64).length, 64);
  });
});
