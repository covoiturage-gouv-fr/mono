import { assertEquals } from "dep:assert";
import { it } from "dep:testing-bdd";

import { StatelessContext } from "../entities/Context.ts";
import { generateCarpool } from "../tests/helpers.ts";
import { onHourRange } from "./onHourRange.ts";

// Europe/Paris in June is UTC+2, so the UTC instant maps to +2h local time.
function setup(datetime: Date) {
  return StatelessContext.fromCarpool(1, generateCarpool({ datetime }));
}

it("should return true inside the range", async () => {
  // 08:00 UTC -> 10:00 Paris
  const ctx = setup(new Date("2025-06-16T08:00:00Z"));
  assertEquals(onHourRange(ctx, { start: 4, end: 23 }), true);
});

it("should return true at the start bound (inclusive)", async () => {
  // 02:00 UTC -> 04:00 Paris
  const ctx = setup(new Date("2025-06-16T02:00:00Z"));
  assertEquals(onHourRange(ctx, { start: 4, end: 23 }), true);
});

it("should return false before the start bound", async () => {
  // 01:00 UTC -> 03:00 Paris
  const ctx = setup(new Date("2025-06-16T01:00:00Z"));
  assertEquals(onHourRange(ctx, { start: 4, end: 23 }), false);
});

it("should return false at the end bound (exclusive)", async () => {
  // 21:00 UTC -> 23:00 Paris
  const ctx = setup(new Date("2025-06-16T21:00:00Z"));
  assertEquals(onHourRange(ctx, { start: 4, end: 23 }), false);
});

it("should respect minutes near the start bound", async () => {
  // 01:30 UTC -> 03:30 Paris
  const ctx = setup(new Date("2025-06-16T01:30:00Z"));
  assertEquals(onHourRange(ctx, { start: 4, end: 23 }), false);
});
