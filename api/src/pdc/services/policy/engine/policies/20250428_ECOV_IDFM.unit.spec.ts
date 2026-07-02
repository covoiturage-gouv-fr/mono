import { describe, it } from "dep:testing-bdd";
import { v4 as uuidV4 } from "@/lib/uuid/index.ts";
import { OperatorsEnum } from "../../interfaces/index.ts";
import { makeProcessHelper } from "../tests/macro.ts";
import { EcovIDFM2026 as Handler } from "./20250428_ECOV_IDFM.ts";

const defaultPosition = {
  arr: "78646",
  com: "78646",
  aom: "287500078", // Île-de-France Mobilités
  epci: "200056232",
  dep: "78",
  reg: "11",
  country: "XXXXX",
};
const defaultLat = 48.72565703413325;
const defaultLon = 2.261827843187402;

// Monday 16/06/2025, 08:00 UTC = 10:00 Paris (summer, UTC+2): weekday, within
// opening hours, not a holiday. Datetimes are UTC-explicit so the tests are
// independent of the runtime timezone (CI runs in UTC).
const defaultCarpool = {
  _id: 1,
  operator_trip_id: uuidV4(),
  passenger_identity_key: uuidV4(),
  driver_identity_key: uuidV4(),
  operator_uuid: OperatorsEnum.ECOV,
  operator_class: "C",
  passenger_is_over_18: true,
  passenger_has_travel_pass: true,
  driver_has_travel_pass: true,
  datetime: new Date("2025-06-16T08:00:00Z"),
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

describe("ECOV x IDFM - Exclusions", () => {
  it("unsupported operator", async () =>
    await process({
      policy: { handler: Handler.id },
      carpool: [{ operator_uuid: "not in list" }],
    }, { incentive: [0] }));

  it("distance below minimum (2 km)", async () =>
    await process({
      policy: { handler: Handler.id },
      carpool: [{ distance: 1_000 }],
    }, { incentive: [0] }));

  it("trip outside AOM (start)", async () =>
    await process({
      policy: { handler: Handler.id },
      carpool: [{ start: { ...defaultPosition, aom: "not_ok" } }],
    }, { incentive: [0] }));

  it("trip outside AOM (end)", async () =>
    await process({
      policy: { handler: Handler.id },
      carpool: [{ end: { ...defaultPosition, aom: "not_ok" } }],
    }, { incentive: [0] }));

  it("operator class not eligible (only C)", async () =>
    await process({
      policy: { handler: Handler.id },
      carpool: [{ operator_class: "B" }],
    }, { incentive: [0] }));

  it("trip before the campaign start (operator not active yet)", async () =>
    await process({
      policy: { handler: Handler.id },
      carpool: [{ datetime: new Date("2025-01-06T09:00:00Z") }],
    }, { incentive: [0] }));

  it("trip on a weekend (Saturday)", async () =>
    await process({
      policy: { handler: Handler.id },
      carpool: [{ datetime: new Date("2025-06-14T08:00:00Z") }],
    }, { incentive: [0] }));

  it("trip before opening hours (03:00)", async () =>
    await process({
      policy: { handler: Handler.id },
      carpool: [{ datetime: new Date("2025-06-16T01:00:00Z") }],
    }, { incentive: [0] }));

  it("trip after closing hours (23:30)", async () =>
    await process({
      policy: { handler: Handler.id },
      carpool: [{ datetime: new Date("2025-06-16T21:30:00Z") }],
    }, { incentive: [0] }));

  it("trip on a public holiday (14 juillet)", async () =>
    await process({
      policy: { handler: Handler.id },
      carpool: [{ datetime: new Date("2025-07-14T08:00:00Z") }],
    }, { incentive: [0] }));
});

describe("ECOV x IDFM - Journée de solidarité", () => {
  it("stays eligible on lundi de Pentecôte (2025-06-09)", async () =>
    await process({
      policy: { handler: Handler.id },
      carpool: [{ datetime: new Date("2025-06-09T08:00:00Z") }],
    }, { incentive: [200] }));
});

describe("ECOV x IDFM - Incentives", () => {
  it("first slice: flat 2€ from 2 to 20 km", async () =>
    await process({
      policy: { handler: Handler.id },
      carpool: [
        { passenger_identity_key: uuidV4(), distance: 2_000 },
        { passenger_identity_key: uuidV4(), distance: 5_000 },
        { passenger_identity_key: uuidV4(), distance: 20_000 },
      ],
    }, { incentive: [200, 200, 200] }));

  it("second slice: +0.10€/km capped at 3€", async () =>
    await process({
      policy: { handler: Handler.id },
      carpool: [
        { passenger_identity_key: uuidV4(), distance: 25_000 },
        { passenger_identity_key: uuidV4(), distance: 30_000 },
        { passenger_identity_key: uuidV4(), distance: 80_000 },
      ],
    }, { incentive: [250, 300, 300] }));
});

describe("ECOV x IDFM - Limits", () => {
  it("limits passengers per trip to 3", async () => {
    const operator_trip_id = uuidV4();
    await process({
      policy: { handler: Handler.id },
      carpool: [
        { driver_identity_key: "1", operator_trip_id, passenger_identity_key: "emile" },
        { driver_identity_key: "1", operator_trip_id, passenger_identity_key: "georgette" },
        { driver_identity_key: "1", operator_trip_id, passenger_identity_key: "bob" },
        { driver_identity_key: "1", operator_trip_id, passenger_identity_key: "alice" }, // exceeds
      ],
    }, { incentive: [200, 200, 200, 0] });
  });

  it("limits passengers per driver per day to 6", async () =>
    await process({
      policy: { handler: Handler.id },
      carpool: [
        { driver_identity_key: "d", operator_trip_id: uuidV4(), passenger_identity_key: "p1", seats: 2 },
        { driver_identity_key: "d", operator_trip_id: uuidV4(), passenger_identity_key: "p2", seats: 2 },
        { driver_identity_key: "d", operator_trip_id: uuidV4(), passenger_identity_key: "p3", seats: 2 },
        { driver_identity_key: "d", operator_trip_id: uuidV4(), passenger_identity_key: "p4", seats: 2 }, // exceeds
      ],
    }, { incentive: [400, 400, 400, 0] }));

  it("limits driver monthly earnings to 200 €", async () =>
    await process({
      policy: { handler: Handler.id },
      carpool: [{ driver_identity_key: "driver_1" }],
      meta: [
        { key: "max_amount_restriction.0-driver_1.month.5-2025", value: 199_00 },
        { key: "max_amount_restriction.global.campaign.global", value: 199_00 },
      ],
    }, { incentive: [1_00] }));
});

describe("ECOV x IDFM - Non-tronçonnage (60 min)", () => {
  it("excludes a second pickup of the same pair within 60 minutes", async () =>
    await process({
      policy: { handler: Handler.id },
      carpool: [
        { driver_identity_key: "d", passenger_identity_key: "p", datetime: new Date("2025-06-16T08:00:00Z") },
        { driver_identity_key: "d", passenger_identity_key: "p", datetime: new Date("2025-06-16T08:30:00Z") },
        { driver_identity_key: "d", passenger_identity_key: "p", datetime: new Date("2025-06-16T10:00:00Z") },
      ],
    }, { incentive: [200, 0, 200] }));
});
