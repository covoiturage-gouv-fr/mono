import { assertEquals } from "dep:assert";
import { describe, it } from "dep:testing-bdd";
import { CSV_COLUMNS, toCsv } from "./csv.ts";

describe("simulation csv", () => {
  const row = {
    operator_id: 9,
    operator_journey_id: "j-1",
    datetime: new Date("2026-10-05T07:30:00Z"),
    start_arr: "34172",
    end_arr: "34327",
    distance: 12000,
  };

  it("writes the header then one line per trip, empty cells when undefined", () => {
    const csv = toCsv([{ ...row, simulated_stateless_amount: 150, simulated_final_amount: 0 }]);
    const [header, line] = csv.split("\n");
    assertEquals(header.split(","), [...CSV_COLUMNS]);
    assertEquals(line, "9,j-1,2026-10-05T07:30:00.000Z,34172,34327,12000,,,150,0,,");
  });

  it("quotes cells with commas, quotes and line breaks", () => {
    const csv = toCsv([{ ...row, rejection_reason: 'too low, "bad"\nlines' }]);
    assertEquals(csv.includes('"too low, ""bad""\nlines"'), true);
  });

  it("holds no personal data column", () => {
    assertEquals(CSV_COLUMNS.some((c) => /identity|phone/.test(c)), false);
  });
});
