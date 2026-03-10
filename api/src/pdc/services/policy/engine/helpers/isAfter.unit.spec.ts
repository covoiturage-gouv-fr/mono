import { assertEquals } from "dep:assert";
import { it } from "dep:testing-bdd";

import { StatelessContext } from "../entities/Context.ts";
import { generateCarpool } from "../tests/helpers.ts";
import { isAfter } from "./isAfter.ts";

function setup(datetime: Date) {
  return StatelessContext.fromCarpool(1, generateCarpool({ datetime }));
}

it("should return false if not in range", async () => {
  const ctx = setup(new Date("2022-01-03"));
  const res = isAfter(ctx, { date: new Date("2022-01-04") });
  assertEquals(res, false);
});

it("should return true if in range", async () => {
  const ctx = setup(new Date("2022-01-03"));
  const res = isAfter(ctx, { date: new Date("2022-01-02") });
  assertEquals(res, true);
});
