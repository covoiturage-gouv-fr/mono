import { assert, assertEquals, assertThrows } from "dep:assert";
import { it } from "dep:testing-bdd";

import { dateCast } from "./dateCast.ts";

it("converts date full ISO", () => {
  assertEquals(
    dateCast()("2019-01-01T00:00:00Z").toISOString(),
    new Date("2019-01-01T00:00:00Z").toISOString(),
  );
  assertEquals(
    dateCast(true)("2019-01-01T00:00:00Z").toISOString(),
    new Date("2019-01-01T00:00:00Z").toISOString(),
  );
});

it("converts date Y-m-d", () => {
  assertEquals(
    dateCast()("2019-01-01").toISOString(),
    new Date("2019-01-01").toISOString(),
  );
  const err1 = assertThrows(() => dateCast(true)("2019-01-01"));
  assert(err1 instanceof Error);
  assertEquals(err1.message, "Invalid Date format");
  const err2 = assertThrows(() => dateCast(true)(1));
  assert(err2 instanceof Error);
  assertEquals(err2.message, "Invalid Date format");
});

it("fails string", () => {
  assertThrows(
    () => dateCast()("not_a_date"),
  );
});

it("fails null", () => {
  assertThrows(() => dateCast()(null as any));
});

it("fails undefined", () => {
  assertThrows(
    () => dateCast()(undefined as any),
  );
});
