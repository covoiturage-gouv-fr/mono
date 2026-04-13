import { assertEquals } from "dep:assert";
import { afterEach, describe, it } from "dep:testing-bdd";
import { getPeriodStart, isPublished } from "./publishedDate.ts";

describe("getPeriodStart", () => {
  it("month=2: returns Feb 1st", () => {
    assertEquals(getPeriodStart({ year: 2026, month: 2 }), new Date(2026, 1, 1));
  });

  it("month=1: returns Jan 1st", () => {
    assertEquals(getPeriodStart({ year: 2026, month: 1 }), new Date(2026, 0, 1));
  });

  it("month=12: returns Dec 1st", () => {
    assertEquals(getPeriodStart({ year: 2026, month: 12 }), new Date(2026, 11, 1));
  });

  it("trimester=1: returns Jan 1st", () => {
    assertEquals(getPeriodStart({ year: 2026, trimester: 1 }), new Date(2026, 0, 1));
  });

  it("trimester=2: returns Apr 1st", () => {
    assertEquals(getPeriodStart({ year: 2026, trimester: 2 }), new Date(2026, 3, 1));
  });

  it("trimester=4: returns Oct 1st", () => {
    assertEquals(getPeriodStart({ year: 2026, trimester: 4 }), new Date(2026, 9, 1));
  });

  it("semester=1: returns Jan 1st", () => {
    assertEquals(getPeriodStart({ year: 2026, semester: 1 }), new Date(2026, 0, 1));
  });

  it("semester=2: returns Jul 1st", () => {
    assertEquals(getPeriodStart({ year: 2026, semester: 2 }), new Date(2026, 6, 1));
  });

  it("year only: returns Jan 1st", () => {
    assertEquals(getPeriodStart({ year: 2026 }), new Date(2026, 0, 1));
  });

  it("month takes priority over trimester", () => {
    assertEquals(
      getPeriodStart({ year: 2026, month: 3, trimester: 1 }),
      new Date(2026, 2, 1),
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

  it("returns true when month starts before cutoff", () => {
    Deno.env.set("APP_OBSERVATORY_PUBLISHED_UNTIL", "2026-03-01");
    assertEquals(isPublished({ year: 2026, month: 2 }), true);
  });

  it("returns false when month starts at cutoff (exclusive)", () => {
    Deno.env.set("APP_OBSERVATORY_PUBLISHED_UNTIL", "2026-03-01");
    assertEquals(isPublished({ year: 2026, month: 3 }), false);
  });

  it("returns false when month starts after cutoff", () => {
    Deno.env.set("APP_OBSERVATORY_PUBLISHED_UNTIL", "2026-03-01");
    assertEquals(isPublished({ year: 2026, month: 6 }), false);
  });

  // Rolling: trimester is included if it starts before cutoff
  it("trimester: Q1 included with cutoff 2026-03-01 (rolling)", () => {
    Deno.env.set("APP_OBSERVATORY_PUBLISHED_UNTIL", "2026-03-01");
    assertEquals(isPublished({ year: 2026, trimester: 1 }), true);
  });

  it("trimester: Q2 excluded with cutoff 2026-03-01", () => {
    Deno.env.set("APP_OBSERVATORY_PUBLISHED_UNTIL", "2026-03-01");
    assertEquals(isPublished({ year: 2026, trimester: 2 }), false);
  });

  // Rolling: semester is included if it starts before cutoff
  it("semester: H1 included with cutoff 2026-03-01 (rolling)", () => {
    Deno.env.set("APP_OBSERVATORY_PUBLISHED_UNTIL", "2026-03-01");
    assertEquals(isPublished({ year: 2026, semester: 1 }), true);
  });

  it("semester: H2 excluded with cutoff 2026-03-01", () => {
    Deno.env.set("APP_OBSERVATORY_PUBLISHED_UNTIL", "2026-03-01");
    assertEquals(isPublished({ year: 2026, semester: 2 }), false);
  });

  // Rolling: year is included if it starts before cutoff
  it("year: 2026 included with cutoff 2026-03-01 (rolling)", () => {
    Deno.env.set("APP_OBSERVATORY_PUBLISHED_UNTIL", "2026-03-01");
    assertEquals(isPublished({ year: 2026 }), true);
  });

  it("year: 2027 excluded with cutoff 2026-03-01", () => {
    Deno.env.set("APP_OBSERVATORY_PUBLISHED_UNTIL", "2026-03-01");
    assertEquals(isPublished({ year: 2027 }), false);
  });

  // Boundary: cutoff at Jan 1 means nothing in that year is visible
  it("boundary: cutoff 2026-01-01, month=1 excluded (starts at cutoff)", () => {
    Deno.env.set("APP_OBSERVATORY_PUBLISHED_UNTIL", "2026-01-01");
    assertEquals(isPublished({ year: 2026, month: 1 }), false);
  });

  it("boundary: cutoff 2026-01-01, year=2025 included", () => {
    Deno.env.set("APP_OBSERVATORY_PUBLISHED_UNTIL", "2026-01-01");
    assertEquals(isPublished({ year: 2025 }), true);
  });

  it("rejects invalid date format gracefully", () => {
    Deno.env.set("APP_OBSERVATORY_PUBLISHED_UNTIL", "not-a-date");
    assertEquals(isPublished({ year: 2026, month: 1 }), false);
  });
});
