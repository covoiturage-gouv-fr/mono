import { assertEquals } from "dep:assert";
import { it } from "dep:testing-bdd";

import { StatelessContext } from "../entities/Context.ts";
import { generateCarpool } from "../tests/helpers.ts";
import { perKm, perSeat } from "./per.ts";

function setup(distance: number, seats: number) {
  return StatelessContext.fromCarpool(1, generateCarpool({ distance, seats }));
}

it("should multiply by seats", async () => {
  const ctx = setup(1, 2);
  const res = perSeat(ctx, 20);
  assertEquals(res, 2 * 20);
});

it("should multiply by seats or one", async () => {
  const ctx = setup(1, 0);
  const res = perSeat(ctx, 20);
  assertEquals(res, 1 * 20);
});

it("should multiply by km", async () => {
  const ctx = setup(10000, 1);
  const res = perKm(ctx, { amount: 20 });
  assertEquals(res, 10 * 20);
});

it("should multiply by 0 km", async () => {
  const ctx = setup(0, 1);
  const res = perKm(ctx, { amount: 20 });
  assertEquals(res, 0);
});

it("should multiply by km with offset", async () => {
  const ctx = setup(10000, 1);
  const res = perKm(ctx, { amount: 20, offset: 3000 });
  assertEquals(res, 7 * 20);
});

it("should multiply by km with limit", async () => {
  const ctx = setup(10000, 1);
  const res = perKm(ctx, { amount: 20, limit: 2000 });
  assertEquals(res, 2 * 20);
});

it("should multiply by km with offset and limit", async () => {
  const ctx = setup(10000, 1);
  const res = perKm(ctx, { amount: 20, offset: 1000, limit: 8000 });
  assertEquals(res, 7 * 20);
});
