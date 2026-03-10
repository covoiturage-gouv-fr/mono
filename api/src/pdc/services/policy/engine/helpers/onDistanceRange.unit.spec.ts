import { assert, assertFalse, assertThrows } from "dep:assert";
import { it } from "dep:testing-bdd";
import { StatelessContext } from "../entities/Context.ts";
import { generateCarpool } from "../tests/helpers.ts";
import { onDistanceRange, onDistanceRangeOrThrow } from "./onDistanceRange.ts";

function distance(distance: number) {
  return StatelessContext.fromCarpool(1, generateCarpool({ distance }));
}

it("should return false if under range", async () => {
  assertFalse(onDistanceRange(distance(10), { min: 14, max: 16 }));
});

it("should return false if above range", async () => {
  assertFalse(onDistanceRange(distance(20), { min: 14, max: 16 }));
});

it("should return true if in range", async () => {
  assert(onDistanceRange(distance(15), { min: 14, max: 16 }));
});

it("should return true if on min", async () => {
  assert(onDistanceRange(distance(14), { min: 14, max: 16 }));
});

it("should return false if on max", async () => {
  assertFalse(onDistanceRange(distance(16), { min: 14, max: 16 }));
});

it("should return true on missing params", () => {
  assert(onDistanceRange(distance(10), {}));
});

it("should return true on missing min param if in range", () => {
  assert(onDistanceRange(distance(10), { max: 15 }));
});

it("should return false on missing min param if not in range", () => {
  assertFalse(onDistanceRange(distance(20), { max: 15 }));
});

it("should return true on missing max param if in range", () => {
  assert(onDistanceRange(distance(10), { min: 5 }));
});

it("should return false on missing max param if not in range", () => {
  assertFalse(onDistanceRange(distance(10), { min: 15 }));
});

it("should not throw if in range", async () => {
  assert(onDistanceRangeOrThrow(distance(15), { min: 14, max: 16 }));
});

it("should throw if not in range", async () => {
  assertThrows(
    () => onDistanceRangeOrThrow(distance(20), { min: 14, max: 16 }),
  );
});

it("should throw on missing distance", () => {
  assertThrows(() => onDistanceRange(distance(undefined as any), { max: 15 }));
});
