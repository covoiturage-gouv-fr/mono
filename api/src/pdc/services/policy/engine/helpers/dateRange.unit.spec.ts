import { assertEquals } from "dep:assert";
import { describe, it } from "dep:testing-bdd";
import { dateRange } from "./dateRange.ts";

describe("dateRange", () => {
  it("should return a range of dates", () => {
    const start = new Date("2022-01-01");
    const end = new Date("2022-01-03");
    const result = dateRange(start, end);
    assertEquals(result, ["2022-01-01", "2022-01-02", "2022-01-03"]);
  });

  it("should return a range of dates when given strings", () => {
    const start = "2022-01-01";
    const end = "2022-01-03";
    const result = dateRange(start, end);
    assertEquals(result, ["2022-01-01", "2022-01-02", "2022-01-03"]);
  });

  it("should return a range over several months", () => {
    const start = "2022-01-30";
    const end = "2022-02-02";
    const result = dateRange(start, end);
    assertEquals(result, ["2022-01-30", "2022-01-31", "2022-02-01", "2022-02-02"]);
  });

  it("should sort input dates", () => {
    const start = "2022-01-03";
    const end = "2022-01-01";
    const result = dateRange(start, end);
    assertEquals(result, ["2022-01-01", "2022-01-02", "2022-01-03"]);
  });

  it("should sort more than two dates", () => {
    const start = "2022-01-03";
    const middle = "2022-01-02";
    const end = "2022-01-01";
    const result = dateRange(start, middle, end);
    assertEquals(result, ["2022-01-01", "2022-01-02", "2022-01-03"]);
  });

  it("should handle no input", () => {
    try {
      dateRange();
    } catch (e) {
      assertEquals(e.message, "At least one date is required");
    }
  });

  it("should handle invalid date format", () => {
    const start = "2022-13-45";
    const end = "2022-01-03";
    try {
      dateRange(start, end);
    } catch (e) {
      assertEquals(e.message, "Invalid Date");
    }
  });

  it("should handle malformed date strings", () => {
    const start = "not-a-date";
    const end = "2022-01-03";
    try {
      dateRange(start, end);
    } catch (e) {
      assertEquals(e.message, "Invalid Date");
    }
  });

  it("should handle empty string input", () => {
    const start = "";
    const end = "2022-01-03";
    try {
      dateRange(start, end);
    } catch (e) {
      assertEquals(e.message, "Invalid Date");
    }
  });

  it("should handle single date input", () => {
    const date = "2022-01-01";
    const result = dateRange(date);
    assertEquals(result, ["2022-01-01"]);
  });

  it("should handle same date for start and end", () => {
    const start = "2022-01-01";
    const end = "2022-01-01";
    const result = dateRange(start, end);
    assertEquals(result, ["2022-01-01"]);
  });

  it("should handle large date ranges", () => {
    const start = "2022-01-01";
    const end = "2022-12-31";
    const result = dateRange(start, end);
    assertEquals(result.length, 365);
  });
});
