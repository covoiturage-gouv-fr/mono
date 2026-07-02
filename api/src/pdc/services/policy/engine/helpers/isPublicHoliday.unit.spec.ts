import { assertEquals } from "dep:assert";
import { it } from "dep:testing-bdd";

import { StatelessContext } from "../entities/Context.ts";
import { generateCarpool } from "../tests/helpers.ts";
import { isPublicHoliday } from "./isPublicHoliday.ts";

function setup(datetime: Date) {
  return StatelessContext.fromCarpool(1, generateCarpool({ datetime }));
}

it("should return true on a fixed-date holiday (14 juillet)", async () => {
  const ctx = setup(new Date("2025-07-14T09:00:00Z"));
  assertEquals(isPublicHoliday(ctx), true);
});

it("should return true on an Easter-based holiday (Ascension 2025)", async () => {
  const ctx = setup(new Date("2025-05-29T09:00:00Z"));
  assertEquals(isPublicHoliday(ctx), true);
});

it("should return true on lundi de Pentecôte (still an official holiday)", async () => {
  const ctx = setup(new Date("2026-05-25T09:00:00Z"));
  assertEquals(isPublicHoliday(ctx), true);
});

it("should return false on a normal working day", async () => {
  const ctx = setup(new Date("2025-07-15T09:00:00Z"));
  assertEquals(isPublicHoliday(ctx), false);
});

it("should use the Paris timezone to resolve the day", async () => {
  // 22:30 UTC on 13/07 -> 00:30 Paris on 14/07 (holiday)
  const ctx = setup(new Date("2025-07-13T22:30:00Z"));
  assertEquals(isPublicHoliday(ctx), true);
});
