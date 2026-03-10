import { assertEquals } from "dep:assert";
import { describe, it } from "dep:testing-bdd";
import { Config, ExportParams } from "./ExportParams.ts";

describe("ExportParams", () => {
  // ----------------------------------------------------------------------------------------
  // SETUP
  // ----------------------------------------------------------------------------------------

  const defaultConfig: Config = {
    start_at: new Date("2019-12-01"),
    end_at: new Date("2020-01-31"),
    operator_id: [],
    geo_selector: { country: ["XXXXX"] },
    tz: "Europe/Paris",
  };

  // ----------------------------------------------------------------------------------------
  // TESTS
  // ----------------------------------------------------------------------------------------

  it("geoToSQL should return start AND end AOM", () => {
    const ep = new ExportParams({
      ...defaultConfig,
      geo_selector: { aom: ["code"] },
    });
    assertEquals(
      ep.geoToSQL("AND"),
      "AND ((gps.aom = 'code') AND (gpe.aom = 'code'))",
    );
  });

  it("geoToSQL should return start OR end AOM", () => {
    const ep = new ExportParams({
      ...defaultConfig,
      geo_selector: { aom: ["code"] },
    });
    assertEquals(
      ep.geoToSQL("OR"),
      "AND ((gps.aom = 'code') OR (gpe.aom = 'code'))",
    );
  });

  it("geoToSQL should return start OR end AOM (default param)", () => {
    const ep = new ExportParams({
      ...defaultConfig,
      geo_selector: { aom: ["code"] },
    });
    assertEquals(
      ep.geoToSQL(),
      "AND ((gps.aom = 'code') OR (gpe.aom = 'code'))",
    );
  });

  it("geoToSQL should return start OR end with many AOM", () => {
    const ep = new ExportParams({
      ...defaultConfig,
      geo_selector: { aom: ["blue", "red"] },
    });
    assertEquals(
      ep.geoToSQL(),
      "AND ((gps.aom = 'blue' OR gps.aom = 'red') OR (gpe.aom = 'blue' OR gpe.aom = 'red'))",
    );
  });

  it("geoToSQL should return start OR end with many AOM and cities", () => {
    const ep = new ExportParams({
      ...defaultConfig,
      geo_selector: { aom: ["blue", "red"], com: ["01010", "2A323"] },
    });
    assertEquals(
      ep.geoToSQL(),
      "AND ((gps.aom = 'blue' OR gps.aom = 'red' OR gps.com = '01010' OR gps.com = '2A323') " +
        "OR (gpe.aom = 'blue' OR gpe.aom = 'red' OR gpe.com = '01010' OR gpe.com = '2A323'))",
    );
  });
});
