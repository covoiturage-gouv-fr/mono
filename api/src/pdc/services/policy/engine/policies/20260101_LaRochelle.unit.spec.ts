import { describe, it } from "dep:testing-bdd";
import { v4 as uuidV4 } from "@/lib/uuid/index.ts";
import { OperatorsEnum } from "../../interfaces/index.ts";
import { makeProcessHelper } from "../tests/macro.ts";
import { LaRochelle2026 as Handler } from "./20260101_LaRochelle.ts";

const defaultPosition = {
  arr: "17059",
  com: "17059",
  aom: "241700434",
  epci: "241700434",
  dep: "17",
  reg: "75",
  country: "XXXXX",
};
const defaultLat = 48.72565703413325;
const defaultLon = 2.261827843187402;

const defaultCarpool = {
  _id: 1,
  operator_trip_id: uuidV4(),
  passenger_identity_key: uuidV4(),
  driver_identity_key: "driver_id_one",
  operator_uuid: OperatorsEnum.BLABLACAR_DAILY,
  operator_class: "C",
  passenger_is_over_18: true,
  passenger_has_travel_pass: true,
  driver_has_travel_pass: true,
  datetime: new Date("2026-02-02"),
  seats: 1,
  distance: 6_000,
  operator_journey_id: uuidV4(),
  operator_id: 9,
  driver_revenue: 20,
  passenger_contribution: 20,
  start: { ...defaultPosition },
  end: { ...defaultPosition },
  start_lat: defaultLat,
  start_lon: defaultLon,
  end_lat: defaultLat,
  end_lon: defaultLon,
};

const process = makeProcessHelper(defaultCarpool);

describe("La Rochelle 2026 - Exclusions", () => {
  it("unsupported operator", async () =>
    await process({
      policy: { handler: Handler.id },
      carpool: [{ operator_uuid: "not in list" }],
    }, { incentive: [0] }));

  it("distance below minimum", async () =>
    await process({
      policy: { handler: Handler.id },
      carpool: [{ distance: 4_800 }],
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

describe("La Rochelle 2026 - Incentives", () => {
  it("first slice (5-10)", async () =>
    await process({
      policy: { handler: Handler.id },
      carpool: [
        { distance: 5_000 },
        { distance: 10_000 },
      ],
    }, { incentive: [100, 100] }));

  it("second slice (10-20)", async () =>
    await process({
      policy: { handler: Handler.id },
      carpool: [
        { distance: 10_000 },
        { distance: 15_000 },
        { distance: 20_000 },
      ],
    }, { incentive: [100, 150, 200] }));

  it("third slice (20-70)", async () =>
    await process({
      policy: { handler: Handler.id },
      carpool: [
        { distance: 20_000 },
        { distance: 30_000 },
        { distance: 70_000 },
      ],
    }, { incentive: [200, 200, 0] }));

  it("farther away", async () =>
    await process({
      policy: { handler: Handler.id },
      carpool: [
        { distance: 70_000 },
        { distance: 100_000 },
      ],
    }, { incentive: [0, 0] }));
});

describe("La Rochelle 2026 - Driver daily limits", () => {
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
    }, { incentive: [100, 100, 100, 100, 100, 100, 0] }));

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
    }, { incentive: [100, 100, 100, 100, 100, 100, 0, 100, 100] }));
});

describe("La Rochelle 2026 - Driver monthly limits", () => {
  it("should limit driver monthly earnings to 80 €", async () => {
    await process({
      policy: { handler: Handler.id },
      carpool: [{ driver_identity_key: "driver_1" }],
      meta: [{
        key: "max_amount_restriction.0-driver_1.month.1-2026",
        value: 79_00,
      }, {
        key: "max_amount_restriction.global.campaign.global",
        value: 79_00,
      }],
    }, {
      incentive: [1_00], // only 1€ left to reach the limit
      meta: [{
        key: "max_amount_restriction.0-driver_1.month.1-2026",
        value: 80_00,
      }, {
        key: "max_amount_restriction.global.campaign.global",
        value: 80_00,
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
        value: 79_99,
      }, {
        key: "max_amount_restriction.global.campaign.global",
        value: 79_99,
      }],
    }, {
      incentive: [
        1, // only 0.01€ left to reach the limit
        1_00, // reset for new month : full incentive
      ],
      meta: [{
        key: "max_amount_restriction.0-driver_1.month.1-2026", // february
        value: 80_00,
      }, {
        key: "max_amount_restriction.global.campaign.global",
        value: 81_00,
      }, {
        key: "max_amount_restriction.0-driver_1.month.2-2026", // march
        value: 1_00,
      }],
    });
  });
});

describe("La Rochelle 2026 - Campaign global limit", () => {
  it("should limit campaign total amount to 210 000 €", async () => {
    await process({
      policy: { handler: Handler.id, max_amount: 210_000_00 },
      carpool: [{}, {}, {}],
      meta: [{
        key: "max_amount_restriction.global.campaign.global",
        value: 209_999_00,
      }],
    }, {
      incentive: [100, 0, 0], // only 1.00€ left to reach the limit
    });
  });
});
