import { assertEquals, assertObjectMatch } from "dep:assert";
import { it } from "dep:testing-bdd";
import { BoundedSlices, UnboundedSlices } from "@/pdc/services/policy/contracts/common/interfaces/Slices.ts";
import { findBoundary, toBoundedSlices, wrapSlices as wrap } from "./wrapSlicesHelper.ts";

it("[wrap] No valid slices returns empty array", () => {
  assertObjectMatch(wrap(undefined), []);
  assertObjectMatch(wrap(null), []);
  assertObjectMatch(wrap([]), []);
  // @ts-expect-error
  assertObjectMatch(wrap("Not an array at all"), []);
});

it("[wrap] Add slices before and after", () => {
  const slices: BoundedSlices = [{ start: 10, end: 20 }];
  const wrapped = wrap(slices);
  assertObjectMatch(wrapped, [{ start: 0, end: 10 }, { start: 10, end: 20 }, {
    start: 20,
  }]);
});

it("[wrap] Add slices after", () => {
  const slices: BoundedSlices = [
    { start: 0, end: 15 },
    { start: 15, end: 30 },
  ];
  const wrapped = wrap(slices);
  assertObjectMatch(wrapped, [{ start: 0, end: 15 }, { start: 15, end: 30 }, {
    start: 30,
  }]);
});

it("[wrap] No additional end slice on Unbounded slices", () => {
  const slices: UnboundedSlices = [{ start: 10, end: 15 }, { start: 15 }];
  const wrapped = wrap(slices);
  assertObjectMatch(wrapped, [{ start: 0, end: 10 }, ...slices]);
});

it("[wrap] No wrapping on Unbounded slices", () => {
  const slices: UnboundedSlices = [{ start: 0, end: 15 }, { start: 15 }];
  const wrapped = wrap(slices);
  assertObjectMatch(wrapped, slices);
});

it("[toBoundedSlices] bounded to bounded", () => {
  const slices: BoundedSlices = [
    { start: 10, end: 15 },
    { start: 15, end: 30 },
  ];
  assertObjectMatch(toBoundedSlices(slices), slices);
});

it("[toBoundedSlices] unbounded to bounded", () => {
  const slices: BoundedSlices = [
    { start: 10, end: 15 },
    { start: 15, end: 30 },
  ];
  assertObjectMatch(
    toBoundedSlices([...slices, { start: 30 }] as UnboundedSlices),
    slices,
  );
});

it("[boundaries] find min and max (sorted)", () => {
  const slices: BoundedSlices = [
    { start: 10, end: 20 },
    { start: 20, end: 30 },
    { start: 30, end: 40 },
  ];

  assertEquals(findBoundary("min", slices), 10);
  assertEquals(findBoundary("max", slices), 40);
});

it("[boundaries] find min and max (unsorted)", () => {
  const slices: BoundedSlices = [
    { start: 30, end: 40 },
    { start: 20, end: 30 },
    { start: 50, end: 100 },
    { start: 10, end: 20 },
  ];

  assertEquals(findBoundary("min", slices), 10);
  assertEquals(findBoundary("max", slices), 100);
});
