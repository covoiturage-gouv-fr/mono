import { describe, it } from "dep:testing-bdd";
import { v4 as uuidV4 } from "@/lib/uuid/index.ts";
import { OperatorsEnum } from "../../interfaces/index.ts";
import { makeProcessHelper } from "../tests/macro.ts";
import { CovoitIDFM2026 as Handler } from "./20251201_Covoit_IDFM_2026.ts";

const defaultPosition = {
  arr: "91377",
  com: "91377",
  aom: "287500078",
  epci: "200056232",
  dep: "91",
  reg: "11",
  country: "XXXXX",
  reseau: 232,
};
const defaultLat = 48.72565703413325;
const defaultLon = 2.261827843187402;

const defaultCarpool = {
  _id: 1,
  operator_trip_id: uuidV4(),
  passenger_identity_key: uuidV4(),
  driver_identity_key: uuidV4(),
  operator_uuid: OperatorsEnum.COVOIT_IDFM,
  operator_class: "C",
  passenger_is_over_18: true,
  passenger_has_travel_pass: true,
  driver_has_travel_pass: true,
  datetime: new Date("2026-02-01"),
  seats: 1,
  distance: 5_000,
  operator_journey_id: uuidV4(),
  operator_id: 1,
  driver_revenue: 2_00,
  passenger_contribution: 0,
  start: { ...defaultPosition },
  end: { ...defaultPosition },
  start_lat: defaultLat,
  start_lon: defaultLon,
  end_lat: defaultLat,
  end_lon: defaultLon,
};

const process = makeProcessHelper(defaultCarpool);

describe("Covoit IDFM 2026 - Exclusions", () => {
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

  it("trip Paris-Paris", async () =>
    await process({
      policy: { handler: Handler.id },
      carpool: [
        {
          start: { ...defaultPosition, com: "75056" },
          end: { ...defaultPosition, com: "75056" },
        },
      ],
    }, { incentive: [0] }));

  it("trip outside AOM", async () =>
    await process({
      policy: { handler: Handler.id },
      carpool: [
        { start: { ...defaultPosition, aom: "not_ok" } },
      ],
    }, { incentive: [0] }));

  it("trip outside AOM (end)", async () =>
    await process({
      policy: { handler: Handler.id },
      carpool: [
        { end: { ...defaultPosition, aom: "not_ok" } },
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

describe("Covoit IDFM 2026 - Incentives", () => {
  it("first slice", async () =>
    await process({
      policy: { handler: Handler.id },
      carpool: [
        { operator_trip_id: uuidV4(), distance: 2_000 },
        { operator_trip_id: uuidV4(), distance: 5_000 },
        { operator_trip_id: uuidV4(), distance: 10_000 },
        { operator_trip_id: uuidV4(), distance: 20_000 },
      ],
    }, { incentive: [200, 200, 200, 200] }));

  it("second slice", async () =>
    await process({
      policy: { handler: Handler.id },
      carpool: [
        { operator_trip_id: uuidV4(), distance: 20_000 },
        { operator_trip_id: uuidV4(), distance: 25_000 },
        { operator_trip_id: uuidV4(), distance: 30_000 },
        { operator_trip_id: uuidV4(), distance: 80_000 },
      ],
    }, { incentive: [200, 250, 300, 300] }));

  it("outside of second slice", async () =>
    await process({
      policy: { handler: Handler.id },
      carpool: [
        { distance: 80_000 },
        { distance: 100_000 },
      ],
    }, { incentive: [300, 300] }));
});

describe("Covoit IDFM 2026 - Driver trip limits", () => {
  it("should limit driver passenger per trip to 3", async () => {
    const operator_trip_id = uuidV4(); // same trip for all carpools

    await process({
      policy: { handler: Handler.id },
      carpool: [
        { driver_identity_key: "1", operator_trip_id, passenger_identity_key: "emile" },
        { driver_identity_key: "1", operator_trip_id, passenger_identity_key: "georgette" },
        { driver_identity_key: "1", operator_trip_id, passenger_identity_key: "bob" },
        { driver_identity_key: "1", operator_trip_id, passenger_identity_key: "alice" }, // sorry Alice
        { driver_identity_key: "1", passenger_identity_key: "jean" }, // different trip, ok
        { driver_identity_key: "2", operator_trip_id: uuidV4(), passenger_identity_key: "marie", seats: 3 }, // Marie & friends
        { driver_identity_key: "2", operator_trip_id: uuidV4(), passenger_identity_key: "paul", seats: 7 }, // Paul & friend (exceeds)
        { driver_identity_key: "2", operator_trip_id: uuidV4(), passenger_identity_key: "marty", seats: 1 }, // Marty alone
      ],
    }, { incentive: [200, 200, 200, 0, 200, 600, 600, 0] });
  });
});

describe("Covoit IDFM 2026 - Driver daily limits", () => {
  it("should limit driver daily trips to 4", async () =>
    await process({
      policy: { handler: Handler.id },
      carpool: [
        { operator_trip_id: uuidV4() },
        { operator_trip_id: uuidV4() },
        { operator_trip_id: uuidV4() },
        { operator_trip_id: uuidV4() },
        { operator_trip_id: uuidV4() }, /* exceeds limit here */
      ],
    }, { incentive: [200, 200, 200, 200, 0] }));

  it("should limit driver daily passengers to 6", async () =>
    await process({
      policy: { handler: Handler.id },
      carpool: [
        { operator_trip_id: uuidV4(), seats: 2 },
        { operator_trip_id: uuidV4(), seats: 2 },
        { operator_trip_id: uuidV4(), seats: 2 },
        { operator_trip_id: uuidV4(), seats: 2 }, // exceeds limit here
      ],
    }, { incentive: [400, 400, 400, 0] }));

  it("should reset limits on different days", async () =>
    await process({
      policy: { handler: Handler.id },
      carpool: [
        { operator_trip_id: uuidV4(), datetime: new Date("2026-02-01T10:00:00") },
        { operator_trip_id: uuidV4(), datetime: new Date("2026-02-01T12:00:00") },
        { operator_trip_id: uuidV4(), datetime: new Date("2026-02-01T14:00:00") },
        { operator_trip_id: uuidV4(), datetime: new Date("2026-02-01T16:00:00") },
        { operator_trip_id: uuidV4(), datetime: new Date("2026-02-01T18:00:00") }, // exceeds limit here

        // next day
        { operator_trip_id: uuidV4(), datetime: new Date("2026-02-02T10:00:00") },
        { operator_trip_id: uuidV4(), datetime: new Date("2026-02-02T12:00:00") },
      ],
    }, { incentive: [200, 200, 200, 200, 0, 200, 200] }));
});

describe("Covoit IDFM 2026 - Driver monthly limits", () => {
  it("should limit driver monthly earnings to 200 €", async () => {
    await process({
      policy: { handler: Handler.id },
      carpool: [{ driver_identity_key: "driver_1" }],
      meta: [{
        key: "max_amount_restriction.0-driver_1.month.1-2026",
        value: 199_00n,
      }, {
        key: "max_amount_restriction.global.campaign.global",
        value: 199_00n,
      }],
    }, {
      incentive: [1_00], // only 1€ left to reach the limit
      meta: [{
        key: "max_amount_restriction.0-driver_1.month.1-2026",
        value: 200_00n,
      }, {
        key: "max_amount_restriction.global.campaign.global",
        value: 200_00n,
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
        value: 199_00n,
      }, {
        key: "max_amount_restriction.global.campaign.global",
        value: 199_00n,
      }],
    }, {
      incentive: [
        1_00, // only 1€ left to reach the limit
        2_00, // reset for new month : full incentive
      ],
      meta: [{
        key: "max_amount_restriction.0-driver_1.month.1-2026", // february
        value: 200_00n,
      }, {
        key: "max_amount_restriction.global.campaign.global",
        value: 202_00n,
      }, {
        key: "max_amount_restriction.0-driver_1.month.2-2026", // march
        value: 2_00n,
      }],
    });
  });
});

describe("Covoit IDFM 2026 - Campaign global limit", () => {
  it("should limit campaign total amount to 16M €", async () => {
    await process({
      policy: { handler: Handler.id, max_amount: 16_000_00_000n },
      carpool: [{}, {}, {}],
      meta: [{
        key: "max_amount_restriction.global.campaign.global",
        value: 15_999_99_900n,
      }],
    }, {
      incentive: [100, 0, 0], // only 0.10€ left to reach the limit
    });
  });
});
