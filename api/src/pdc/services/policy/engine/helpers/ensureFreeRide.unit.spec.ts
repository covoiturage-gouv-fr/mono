import { assertEquals } from "dep:assert";
import { it } from "dep:testing-bdd";

import { StatelessContext } from "../entities/Context.ts";
import { generateCarpool } from "../tests/helpers.ts";
import { ensureFreeRide } from "./ensureFreeRide.ts";

function setup(driver_revenue: number) {
  return StatelessContext.fromCarpool(1, generateCarpool({ driver_revenue }));
}

it("should be equal to difference", async () => {
  const ctx = setup(100);
  const res = ensureFreeRide(ctx, 20);
  assertEquals(res, 80);
});

it("should be null", async () => {
  const ctx = setup(100);
  const res = ensureFreeRide(ctx, 120);
  assertEquals(res, 0);
});
