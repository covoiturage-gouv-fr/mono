import { assertEquals } from "dep:assert";
import { it } from "dep:testing-bdd";
import {
  addDaysTz,
  addMinutes,
  addMonthsTz,
  castUserStringToUTC,
  differenceInHours,
  startOfMonthTz,
  subDaysTz,
  toISOString,
  toTzString,
} from "./dates.helper.ts";

function toTzStringParis(d: Date): string {
  return toTzString(d, "Europe/Paris");
}

it("marginals", () => {
  assertEquals(castUserStringToUTC("Europe/Paris", undefined), undefined);
  assertEquals(castUserStringToUTC("Europe/Paris", null), undefined);
  // @ts-expect-error
  assertEquals(castUserStringToUTC("Europe/Paris", "kjahdkjhsad"), undefined);
});

it("full form Europe/Paris", () => {
  assertEquals(
    toISOString(
      castUserStringToUTC("2023-01-01T00:00:00+0100", "Europe/Paris"),
    ),
    "2022-12-31T23:00:00.000Z",
  );
  assertEquals(
    toTzStringParis(
      castUserStringToUTC("2023-01-01T00:00:00+0100", "Europe/Paris"),
    ),
    "2023-01-01T00:00:00+0100",
  );
});

it("full form Indian/Reunion", () => {
  assertEquals(
    toISOString(
      castUserStringToUTC("2023-01-01T00:00:00+0400", "Indian/Reunion"),
    ),
    "2022-12-31T20:00:00.000Z",
  );
  assertEquals(
    toTzString(
      castUserStringToUTC("2023-01-01T00:00:00+0400", "Indian/Reunion"),
      "Indian/Reunion",
    ),
    "2023-01-01T00:00:00+0400",
  );
});

it("short form Europe/Paris", () => {
  assertEquals(
    toISOString(castUserStringToUTC("2023-01-01", "Europe/Paris")),
    "2022-12-31T23:00:00.000Z",
  );
  assertEquals(
    toTzStringParis(castUserStringToUTC("2023-01-01", "Europe/Paris")),
    "2023-01-01T00:00:00+0100",
  );
});

it("short form Indian/Reunion", () => {
  assertEquals(
    toISOString(castUserStringToUTC("2023-01-01", "Indian/Reunion")),
    "2022-12-31T20:00:00.000Z",
  );
  assertEquals(
    toTzString(
      castUserStringToUTC("2023-01-01", "Indian/Reunion"),
      "Indian/Reunion",
    ),
    "2023-01-01T00:00:00+0400",
  );
});

it("short form Europe/Paris - add/sub days", () => {
  const start = castUserStringToUTC("2023-01-01", "Europe/Paris");
  assertEquals(toISOString(start), "2022-12-31T23:00:00.000Z");
  assertEquals(toTzStringParis(start), "2023-01-01T00:00:00+0100");

  assertEquals(toISOString(addDaysTz(start, 7)), "2023-01-07T23:00:00.000Z");
  assertEquals(
    toTzStringParis(addDaysTz(start, 7)),
    "2023-01-08T00:00:00+0100",
  );

  assertEquals(toISOString(subDaysTz(start, 7)), "2022-12-24T23:00:00.000Z");
  assertEquals(
    toTzStringParis(subDaysTz(start, 7)),
    "2022-12-25T00:00:00+0100",
  );
});

it("short form Europe/Paris - add months", () => {
  const start = castUserStringToUTC("2023-01-01", "Europe/Paris");
  assertEquals(toISOString(start), "2022-12-31T23:00:00.000Z");
  assertEquals(toTzStringParis(start), "2023-01-01T00:00:00+0100");

  assertEquals(toISOString(addMonthsTz(start, 5)), "2023-05-31T22:00:00.000Z");
  assertEquals(
    toTzStringParis(addMonthsTz(start, 5)),
    "2023-06-01T00:00:00+0200",
  );

  assertEquals(toISOString(addMonthsTz(start, 12)), "2023-12-31T23:00:00.000Z");
  assertEquals(
    toTzStringParis(addMonthsTz(start, 12)),
    "2024-01-01T00:00:00+0100",
  );
});

it("short form Europe/Paris - startOfMonth 01/01", () => {
  const start = castUserStringToUTC("2023-01-01", "Europe/Paris");
  assertEquals(toISOString(start), "2022-12-31T23:00:00.000Z");
  assertEquals(toTzStringParis(start), "2023-01-01T00:00:00+0100");

  assertEquals(toISOString(startOfMonthTz(start)), "2022-12-31T23:00:00.000Z");
  assertEquals(
    toTzStringParis(startOfMonthTz(start)),
    "2023-01-01T00:00:00+0100",
  );
});

it("short form Europe/Paris - startOfMonth 05/01", () => {
  const start = castUserStringToUTC("2023-01-05", "Europe/Paris");
  assertEquals(toISOString(start), "2023-01-04T23:00:00.000Z");
  assertEquals(toTzStringParis(start), "2023-01-05T00:00:00+0100");

  assertEquals(toISOString(startOfMonthTz(start)), "2022-12-31T23:00:00.000Z");
  assertEquals(
    toTzStringParis(startOfMonthTz(start)),
    "2023-01-01T00:00:00+0100",
  );
});

it("differenceInHours returns fractional hours", () => {
  const created_at = new Date("2024-10-24 06:37:09");
  const start_datetime = new Date("2024-10-23 05:00:47");
  assertEquals(differenceInHours(created_at, start_datetime), 25.60611111111111);
});

it("addMinutes adds positive minutes", () => {
  const d = new Date("2024-01-01T12:00:00Z");
  assertEquals(addMinutes(d, 30).toISOString(), "2024-01-01T12:30:00.000Z");
});

it("addMinutes subtracts with negative minutes", () => {
  const d = new Date("2024-01-01T12:00:00Z");
  assertEquals(addMinutes(d, -30).toISOString(), "2024-01-01T11:30:00.000Z");
});

it("short form Europe/Paris - next startOfMonth 05/01", () => {
  const start = castUserStringToUTC("2023-01-05", "Europe/Paris");
  assertEquals(toISOString(start), "2023-01-04T23:00:00.000Z");
  assertEquals(toTzStringParis(start), "2023-01-05T00:00:00+0100");

  assertEquals(
    toISOString(addMonthsTz(startOfMonthTz(start), 1)),
    "2023-01-31T23:00:00.000Z",
  );
  assertEquals(
    toTzStringParis(addMonthsTz(startOfMonthTz(start), 1)),
    "2023-02-01T00:00:00+0100",
  );
});
