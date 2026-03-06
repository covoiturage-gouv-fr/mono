import { assertEquals } from "dep:assert";
import { it } from "dep:testing-bdd";
import { currency } from "./currency.ts";

it("should work", () => {
  assertEquals(currency("10.1"), "10,10");
  assertEquals(currency("not_a_number"), "0,00");
});
