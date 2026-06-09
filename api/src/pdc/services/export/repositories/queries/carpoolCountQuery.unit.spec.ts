import { assert, assertStringIncludes } from "dep:assert";
import { describe, it } from "dep:testing-bdd";
import { ExportParams } from "@/pdc/services/export/models/ExportParams.ts";
import { carpoolCountQuery } from "./carpoolCountQuery.ts";

describe("carpoolCountQuery", () => {
  const params = new ExportParams({
    start_at: new Date("2026-03-01T00:00:00Z"),
    end_at: new Date("2026-04-01T00:00:00Z"),
    operator_id: [],
    geo_selector: null,
    tz: "Europe/Paris",
  });

  it("keeps the exact start_date_filter half-open range", () => {
    const { text } = carpoolCountQuery(params);
    assertStringIncludes(text, "cc.start_date_filter >= $");
    assertStringIncludes(text, "cc.start_date_filter  < $");
  });

  it("adds a sargable start_datetime_utc range hitting the journeys index", () => {
    const { text } = carpoolCountQuery(params);
    assertStringIncludes(text, "cc.start_datetime_utc >= (");
    assertStringIncludes(text, "cc.start_datetime_utc  < (");
    assertStringIncludes(text, "AT TIME ZONE 'Europe/Paris'");
    assert(!/\w+\(\s*cc\.start_datetime_utc/.test(text), "start_datetime_utc must not be wrapped in a function");
  });
});
