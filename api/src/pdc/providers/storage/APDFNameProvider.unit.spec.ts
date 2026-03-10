import { assertEquals, assertObjectMatch } from "dep:assert";
import { describe, it } from "dep:testing-bdd";
import { getTmpDir } from "@/lib/file/index.ts";
import { join } from "@/lib/path/index.ts";
import { APDFNameProvider } from "./APDFNameProvider.ts";

describe("APDFNameProvider", () => {
  it("filename", () => {
    const provider = new APDFNameProvider();
    assertEquals(
      provider.filename({
        name: "YOLO",
        datetime: new Date("2022-01-01T00:00:00Z"),
        campaign_id: 1,
        operator_id: 2,
        trips: 111,
        subsidized: 100,
        amount: 222_00,
      }),
      "APDF-2022-01-1-2-111-100-22200-yolo.xlsx",
    );
  });

  it("filename null trips and amount", () => {
    const provider = new APDFNameProvider();
    assertEquals(
      provider.filepath({
        name: "YOLO",
        datetime: new Date("2022-01-01T00:00:00Z"),
        campaign_id: 1,
        operator_id: 2,
        trips: 0,
        subsidized: 0,
        amount: 0,
      }),
      join(getTmpDir(), "APDF-2022-01-1-2-0-0-0-yolo.xlsx"),
    );
  });

  it("filename rounded amount", () => {
    const provider = new APDFNameProvider();
    assertEquals(
      provider.filepath({
        name: "YOLO",
        datetime: new Date("2022-01-01T00:00:00Z"),
        campaign_id: 1,
        operator_id: 2,
        trips: 111,
        subsidized: 100,
        amount: 222_99,
      }),
      join(getTmpDir(), "APDF-2022-01-1-2-111-100-22299-yolo.xlsx"),
    );
  });

  it("filepath with string", () => {
    const provider = new APDFNameProvider();
    assertEquals(
      provider.filepath("APDF-2022-01-1-2-3-4-yolo.xlsx"),
      join(getTmpDir(), "APDF-2022-01-1-2-3-4-yolo.xlsx"),
    );
  });

  it("filepath with object", () => {
    const provider = new APDFNameProvider();
    assertEquals(
      provider.filepath({
        name: "YOLO",
        datetime: new Date("2022-01-01T00:00:00Z"),
        campaign_id: 1,
        operator_id: 2,
        trips: 111,
        subsidized: 100,
        amount: 222_00,
      }),
      join(getTmpDir(), "APDF-2022-01-1-2-111-100-22200-yolo.xlsx"),
    );
  });

  it("Parse APDF: filename", () => {
    const provider = new APDFNameProvider();
    assertObjectMatch(
      provider.parse("APDF-2022-01-1-2-111-100-22200-abc123.xlsx"),
      {
        name: "abc123",
        datetime: new Date("2022-01-01T00:00:00Z"),
        campaign_id: 1,
        operator_id: 2,
        trips: 111,
        subsidized: 100,
        amount: 222_00,
      },
    );
  });

  it("Parse APDF: filename with prefix", () => {
    const provider = new APDFNameProvider();
    assertObjectMatch(
      provider.parse("1/APDF-2022-01-1-2-111-100-22200-abc123.xlsx"),
      {
        name: "abc123",
        datetime: new Date("2022-01-01T00:00:00Z"),
        campaign_id: 1,
        operator_id: 2,
        trips: 111,
        subsidized: 100,
        amount: 222_00,
      },
    );
  });
});
