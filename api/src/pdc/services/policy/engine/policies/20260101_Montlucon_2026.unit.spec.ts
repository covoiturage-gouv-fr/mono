import { describe, it } from "dep:testing-bdd";
import { v4 as uuidV4 } from "@/lib/uuid/index.ts";
import { OperatorsEnum } from "../../interfaces/index.ts";
import { makeProcessHelper } from "../tests/macro.ts";
import { MontluconCommunaute2026 as Handler } from "./20260101_Montlucon_2026.ts";

const defaultPosition = {
  arr: "03007",
  com: "03007",
  aom: "200071082",
  epci: "200071082",
  dep: "03",
  reg: "84",
  country: "XXXXX",
  reseau: 1,
};
const defaultLat = 46.22839350278345;
const defaultLon = 2.652753826894522;

const defaultCarpool = {
  _id: 1,
  operator_trip_id: uuidV4(),
  passenger_identity_key: uuidV4(),
  driver_identity_key: uuidV4(),
  operator_uuid: OperatorsEnum.BLABLACAR_DAILY,
  operator_class: "C",
  passenger_is_over_18: true,
  passenger_has_travel_pass: true,
  driver_has_travel_pass: true,
  datetime: new Date("2026-02-01"),
  seats: 1,
  distance: 5_000,
  operator_journey_id: uuidV4(),
  operator_id: 1,
  driver_revenue: 1_50,
  passenger_contribution: 0,
  start: { ...defaultPosition },
  end: { ...defaultPosition },
  start_lat: defaultLat,
  start_lon: defaultLon,
  end_lat: defaultLat,
  end_lon: defaultLon,
};

const process = makeProcessHelper(defaultCarpool);

describe("MontluconCommunaute2026 - Exclusions", () => {
  it("unsupported operator", async () =>
    await process({
      policy: { handler: Handler.id },
      carpool: [{ operator_uuid: "not in list" }],
    }, { incentive: [0] }));

  it("distance below minimum", async () =>
    await process({
      policy: { handler: Handler.id },
      carpool: [{ distance: 1_000 }],
    }, { incentive: [0] }));

  it("trip outside AOM", async () =>
    await process({
      policy: { handler: Handler.id },
      carpool: [
        {
          start: { ...defaultPosition, aom: "not_ok" },
          end: { ...defaultPosition, aom: "not_ok" },
        },
      ],
    }, { incentive: [0] }));

  it("operator class not eligible", async () =>
    await process({
      policy: { handler: Handler.id },
      carpool: [{ operator_class: "A" }],
    }, { incentive: [0] }));

  it("trip outside of campaign dates", async () =>
    await process({
      policy: { handler: Handler.id },
      carpool: [{ datetime: new Date("2022-09-02") }],
    }, { incentive: [0] }));
});

describe("Montluçon 2026 - Incentives", () => {
  it("first slice (2-15)", async () =>
    await process({
      policy: { handler: Handler.id },
      carpool: [
        { distance: 2_000 },
        { distance: 5_000 },
        { distance: 10_000 },
        { distance: 15_000 },
      ],
    }, { incentive: [150, 150, 150, 150] }));

  it("second slice (15-40)", async () =>
    await process({
      policy: { handler: Handler.id },
      carpool: [
        { distance: 15_000 },
        { distance: 20_000 },
        { distance: 25_000 },
        { distance: 30_000 },
        { distance: 40_000 },
      ],
    }, { incentive: [150, 200, 250, 300, 400] }));

  it("third slice (40-60)", async () =>
    await process({
      policy: { handler: Handler.id },
      carpool: [
        { distance: 40_000 },
        { distance: 50_000 },
        { distance: 60_000 },
      ],
    }, { incentive: [400, 400, 400] }));

  it("decreasing slice (60-80)", async () =>
    await process({
      policy: { handler: Handler.id },
      carpool: [
        { distance: 60_000 },
        { distance: 65_000 },
        { distance: 70_000 },
        { distance: 75_000 },
        { distance: 80_000 },
      ],
    }, { incentive: [400, 300, 200, 100, 0] }));

  it("farther away", async () =>
    await process({
      policy: { handler: Handler.id },
      carpool: [
        { distance: 80_000 },
        { distance: 100_000 },
      ],
    }, { incentive: [0, 0] }));
});

describe("Montluçon 2026 - Driver daily limits", () => {
  it("should limit driver daily trips to 6", async () =>
    await process({
      policy: { handler: Handler.id },
      carpool: [
        { operator_trip_id: uuidV4() },
        { operator_trip_id: uuidV4() },
        { operator_trip_id: uuidV4() },
        { operator_trip_id: uuidV4() },
        { operator_trip_id: uuidV4() },
        { operator_trip_id: uuidV4() },
        { operator_trip_id: uuidV4() }, /* exceeds limit here */
      ],
    }, { incentive: [150, 150, 150, 150, 150, 150, 0] }));

  it("should reset limits on different days", async () =>
    await process({
      policy: { handler: Handler.id },
      carpool: [
        { datetime: new Date("2026-02-01T10:00:00") },
        { datetime: new Date("2026-02-01T12:00:00") },
        { datetime: new Date("2026-02-01T14:00:00") },
        { datetime: new Date("2026-02-01T16:00:00") },
        { datetime: new Date("2026-02-01T16:00:00") },
        { datetime: new Date("2026-02-01T16:00:00") },
        { datetime: new Date("2026-02-01T18:00:00") }, // exceeds limit here

        // next day
        { datetime: new Date("2026-02-02T10:00:00") },
        { datetime: new Date("2026-02-02T12:00:00") },
      ],
    }, { incentive: [150, 150, 150, 150, 150, 150, 0, 150, 150] }));
});

describe("Montluçon 2026 - Driver monthly limits", () => {
  it("should limit driver monthly earnings to 150 €", async () => {
    await process({
      policy: { handler: Handler.id },
      carpool: [{ driver_identity_key: "driver_1" }],
      meta: [{
        key: "max_amount_restriction.0-driver_1.month.1-2026",
        value: 149_00,
      }, {
        key: "max_amount_restriction.global.campaign.global",
        value: 149_00,
      }],
    }, {
      incentive: [1_00], // only 1€ left to reach the limit
      meta: [{
        key: "max_amount_restriction.0-driver_1.month.1-2026",
        value: 150_00,
      }, {
        key: "max_amount_restriction.global.campaign.global",
        value: 150_00,
      }],
    });
  });

  it("should reset limits on next month", async () => {
    await process({
      policy: { handler: Handler.id },
      carpool: [
        { driver_identity_key: "driver_1", datetime: new Date("2026-02-10") },
        { driver_identity_key: "driver_1", datetime: new Date("2026-03-10") },
      ],
      meta: [{
        key: "max_amount_restriction.0-driver_1.month.1-2026",
        value: 149_00,
      }, {
        key: "max_amount_restriction.global.campaign.global",
        value: 149_00,
      }],
    }, {
      incentive: [
        1_00, // only 1€ left to reach the limit
        1_50, // reset for new month : full incentive
      ],
      meta: [{
        key: "max_amount_restriction.0-driver_1.month.1-2026", // february
        value: 150_00,
      }, {
        key: "max_amount_restriction.global.campaign.global",
        value: 151_50,
      }, {
        key: "max_amount_restriction.0-driver_1.month.2-2026", // march
        value: 1_50,
      }],
    });
  });
});

describe("Montluçon 2026 - Campaign global limit", () => {
  it("should limit campaign total amount to 26 344 €", async () => {
    await process({
      policy: { handler: Handler.id, max_amount: 26_344_00 },
      carpool: [{}, {}, {}],
      meta: [{
        key: "max_amount_restriction.global.campaign.global",
        value: 26_343_00,
      }],
    }, {
      incentive: [100, 0, 0], // only 1.00€ left to reach the limit
    });
  });
});
