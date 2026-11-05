import { assertEquals } from "dep:assert";
import { it } from "dep:testing-bdd";

import { TerritoryCodeInterface } from "../../interfaces/index.ts";
import { isSelected } from "./isSelected.ts";

it("should return true if code is selected", () => {
  assertEquals(isSelected({ aom: "217500016" }, { aom: ["217500016"] }), true);
});

it("should return false if code is not selected", () => {
  assertEquals(isSelected({ aom: "217500016" }, { aom: ["200053791"] }), false);
});

it("should return false if there is no territory", () => {
  assertEquals(isSelected(null as unknown as TerritoryCodeInterface, { aom: ["217500016"] }), false);
});
