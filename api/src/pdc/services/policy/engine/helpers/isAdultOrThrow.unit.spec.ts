import { assertEquals, assertThrows } from "dep:assert";
import { it } from "dep:testing-bdd";

import { StatelessContext } from "../entities/Context.ts";
import { generateCarpool } from "../tests/helpers.ts";
import { isAdultOrThrow } from "./isAdultOrThrow.ts";

function setup(passenger_is_over_18: boolean) {
  return StatelessContext.fromCarpool(
    1,
    generateCarpool({ passenger_is_over_18 }),
  );
}

it("should throw if not an adult", async () => {
  const ctx = setup(false);
  assertThrows(() => {
    isAdultOrThrow(ctx);
  });
});

it("should return true if adult", async () => {
  const ctx = setup(true);
  const res = isAdultOrThrow(ctx);
  assertEquals(res, true);
});
