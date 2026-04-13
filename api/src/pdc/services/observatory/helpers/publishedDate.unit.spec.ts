import { assertEquals } from "dep:assert";
import { afterEach, describe, it } from "dep:testing-bdd";
import { getPeriodEnd, isPublished } from "./publishedDate.ts";

describe("getPeriodEnd", () => {
  it("month: returns first day of next month", () => {
    assertEquals(getPeriodEnd({ year: 2026, month: 2 }), new Date(2026, 2, 1));
  });

  it("month=12: rolls over to next year", () => {
    assertEquals(getPeriodEnd({ year: 2026, month: 12 }), new Date(2027, 0, 1));
  });

  it("trimester=1: Q1 ends at April", () => {
    assertEquals(getPeriodEnd({ year: 2026, trimester: 1 }), new Date(2026, 3, 1));
  });

  it("trimester=4: Q4 rolls to next year", () => {
    assertEquals(getPeriodEnd({ year: 2026, trimester: 4 }), new Date(2027, 0, 1));
  });

  it("semester=1: H1 ends at July", () => {
    assertEquals(getPeriodEnd({ year: 2026, semester: 1 }), new Date(2026, 6, 1));
  });

  it("semester=2: H2 rolls to next year", () => {
    assertEquals(getPeriodEnd({ year: 2026, semester: 2 }), new Date(2027, 0, 1));
  });

  it("year only: returns Jan 1 of next year", () => {
    assertEquals(getPeriodEnd({ year: 2026 }), new Date(2027, 0, 1));
  });

  it("month takes priority over trimester", () => {
    assertEquals(
      getPeriodEnd({ year: 2026, month: 3, trimester: 1 }),
      new Date(2026, 3, 1),
    );
  });
});

describe("isPublished", () => {
  afterEach(() => {
    Deno.env.delete("APP_OBSERVATORY_PUBLISHED_UNTIL");
  });

  it("returns true when env var is unset", () => {
    Deno.env.delete("APP_OBSERVATORY_PUBLISHED_UNTIL");
    assertEquals(isPublished({ year: 2099, month: 12 }), true);
  });

  it("returns true when period is before cutoff", () => {
    Deno.env.set("APP_OBSERVATORY_PUBLISHED_UNTIL", "2026-03-01");
    assertEquals(isPublished({ year: 2026, month: 2 }), true);
  });

  it("returns false when period equals cutoff", () => {
    Deno.env.set("APP_OBSERVATORY_PUBLISHED_UNTIL", "2026-03-01");
    assertEquals(isPublished({ year: 2026, month: 3 }), false);
  });

  it("returns false when period is after cutoff", () => {
    Deno.env.set("APP_OBSERVATORY_PUBLISHED_UNTIL", "2026-03-01");
    assertEquals(isPublished({ year: 2026, month: 6 }), false);
  });

  it("works with trimester", () => {
    Deno.env.set("APP_OBSERVATORY_PUBLISHED_UNTIL", "2026-04-01");
    assertEquals(isPublished({ year: 2026, trimester: 1 }), true);
    assertEquals(isPublished({ year: 2026, trimester: 2 }), false);
  });

  it("works with semester", () => {
    Deno.env.set("APP_OBSERVATORY_PUBLISHED_UNTIL", "2026-07-01");
    assertEquals(isPublished({ year: 2026, semester: 1 }), true);
    assertEquals(isPublished({ year: 2026, semester: 2 }), false);
  });

  it("works with year only", () => {
    Deno.env.set("APP_OBSERVATORY_PUBLISHED_UNTIL", "2027-01-01");
    assertEquals(isPublished({ year: 2026 }), true);
    assertEquals(isPublished({ year: 2027 }), false);
  });

  it("boundary: cutoff exactly at period end is published", () => {
    Deno.env.set("APP_OBSERVATORY_PUBLISHED_UNTIL", "2026-04-01");
    assertEquals(isPublished({ year: 2026, trimester: 1 }), true);
  });

  it("rejects invalid date format gracefully", () => {
    Deno.env.set("APP_OBSERVATORY_PUBLISHED_UNTIL", "not-a-date");
    assertEquals(isPublished({ year: 2026, month: 1 }), false);
  });
});
