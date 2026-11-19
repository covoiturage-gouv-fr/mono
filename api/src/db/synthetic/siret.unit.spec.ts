import { assert, assertEquals, assertFalse, assertThrows } from "dep:assert";
import { describe, it } from "dep:testing-bdd";
import { isLuhnValid, isValidSiret, luhnKey } from "./siret.ts";

describe("synthetic / siret", () => {
  it("validates known numbers", () => {
    assert(isLuhnValid("130025265")); // SIREN DINUM
    assert(isValidSiret("13002526500013"));
    assertFalse(isLuhnValid("130025266"));
    assertFalse(isValidSiret("13002526500014"));
    assertFalse(isValidSiret("1300252650001")); // 13 chiffres
    assertFalse(isLuhnValid("13002526A"));
  });

  it("computes the check digit", () => {
    assertEquals(luhnKey("13002526"), "5");
    assertEquals(luhnKey("1300252650001"), "3");
    for (const base of ["00000001", "00000002", "12345678", "99999999"]) {
      assert(isLuhnValid(base + luhnKey(base)), base);
    }
    assertThrows(() => luhnKey("12A"));
  });
});
