import { assertEquals } from "dep:assert";
import { it } from "dep:testing-bdd";
import sinon from "dep:sinon";
import { todayFrequencies } from "./todayFrequencies.helper.ts";

it("On Monday", () => {
  const clock = sinon.useFakeTimers(new Date("2023-02-13"));
  assertEquals(todayFrequencies(), ["daily", "weekly"]);
  clock.restore();
});

it("On Tuesday", () => {
  const clock = sinon.useFakeTimers(new Date("2023-02-14"));
  assertEquals(todayFrequencies(), ["daily"]);
  clock.restore();
});

it("On 1st day of the month (not a Monday)", () => {
  const clock = sinon.useFakeTimers(new Date("2023-02-01"));
  assertEquals(todayFrequencies(), ["daily", "monthly"]);
  clock.restore();
});

it("On 1st day of the month (a Monday)", () => {
  const clock = sinon.useFakeTimers(new Date("2022-08-01"));
  assertEquals(todayFrequencies(), ["daily", "weekly", "monthly"]);
  clock.restore();
});
