import { describe, it } from "dep:testing-bdd";
import { v4 as uuidV4 } from "@/lib/uuid/index.ts";
import { CarpoolInterface, OperatorsEnum } from "../../interfaces/index.ts";
import { makeProcessHelper } from "../tests/macro.ts";
import { GrandChatellerault2026 as Handler } from "./20260101_GrandChatellerault.ts";

// Châtellerault
const defaultPosition = {
  arr: "86066",
  com: "86066",
  aom: "248600413",
  epci: "248600413",
  dep: "86",
  reg: "75",
  country: "XXXXX",
  reseau: 0,
};

const defaultLat = 46.81522088560449;
const defaultLon = 0.5471076683365941;

const defaultCarpool: CarpoolInterface = {
  passenger_contribution: 20,
  passenger_identity_key: uuidV4(),
  passenger_has_travel_pass: true,
  passenger_is_over_18: true,
  driver_revenue: 20,
  driver_identity_key: uuidV4(),
  driver_has_travel_pass: true,
  operator_journey_id: uuidV4(),
  operator_id: 1,
  operator_trip_id: uuidV4(),
  operator_uuid: OperatorsEnum.KAROS,
  operator_class: "C",
  datetime: new Date("2026-02-01"),
  seats: 1,
  distance: 3_000,
  start: { ...defaultPosition },
  start_lat: defaultLat,
  start_lon: defaultLon,
  end: { ...defaultPosition },
  end_lat: defaultLat,
  end_lon: defaultLon,
};

const process = makeProcessHelper(defaultCarpool);

describe("Grand Châtellerault 2026 - Exclusions", () => {
  it("unsupported operator", async () =>
    await process({
      policy: { handler: Handler.id },
      carpool: [{ operator_uuid: "not in list" }],
    }, { incentive: [0] }));

  it("distance below minimum (< 3km)", async () =>
    await process({
      policy: { handler: Handler.id },
      carpool: [{ distance: 2_999 }],
    }, { incentive: [0] }));

  it("distance above maximum (> 80km)", async () =>
    await process({
      policy: { handler: Handler.id },
      carpool: [{ distance: 80_001 }],
    }, { incentive: [0] }));

  it("operator class not eligible", async () =>
    await process({
      policy: { handler: Handler.id },
      carpool: [{ operator_class: "A" }],
    }, { incentive: [0] }));

  it("passenger is minor", async () =>
    await process({
      policy: { handler: Handler.id },
      carpool: [{ passenger_is_over_18: false }],
    }, { incentive: [0] }));

  it("OD outside AOM", async () =>
    await process({
      policy: { handler: Handler.id },
      carpool: [
        {
          start: { ...defaultPosition, aom: "244900015" },
          end: { ...defaultPosition, aom: "244900015" },
        },
      ],
    }, { incentive: [0] }));

  it("Origine from excluded AOM (Grand Poitiers)", async () =>
    await process({
      policy: { handler: Handler.id },
      carpool: [
        {
          start: { ...defaultPosition, aom: "200069854" }, // start is excluded
          end: { ...defaultPosition },
        },
        {
          start: { ...defaultPosition },
          end: { ...defaultPosition, aom: "200069854" }, // end is fine
        },
      ],
    }, { incentive: [0, 150] }));
});

describe("Grand Châtellerault 2026 - Incentives", () => {
  it("150 centimes for eligible trip (3-80 km)", async () =>
    await process({
      policy: { handler: Handler.id },
      carpool: [
        { passenger_identity_key: uuidV4(), distance: 3_000 },
        { passenger_identity_key: uuidV4(), distance: 10_000 },
        { passenger_identity_key: uuidV4(), distance: 50_000 },
        { passenger_identity_key: uuidV4(), distance: 79_999 },
      ],
    }, { incentive: [150, 150, 150, 150] }));

  it("0 for trips outside range", async () =>
    await process({
      policy: { handler: Handler.id },
      carpool: [
        { distance: 2_999 },
        { distance: 80_001 },
      ],
    }, { incentive: [0, 0] }));

  it("many seats", async () =>
    await process({
      policy: { handler: Handler.id },
      carpool: [
        { distance: 5_000, seats: 2 },
      ],
    }, { incentive: [300] }));
});

describe("Grand Châtellerault 2026 - Driver daily limits", () => {
  it("should limit driver daily trips to 6", async () =>
    await process({
      policy: { handler: Handler.id },
      carpool: [
        { passenger_identity_key: uuidV4(), operator_trip_id: uuidV4() },
        { passenger_identity_key: uuidV4(), operator_trip_id: uuidV4() },
        { passenger_identity_key: uuidV4(), operator_trip_id: uuidV4() },
        { passenger_identity_key: uuidV4(), operator_trip_id: uuidV4() },
        { passenger_identity_key: uuidV4(), operator_trip_id: uuidV4() },
        { passenger_identity_key: uuidV4(), operator_trip_id: uuidV4() },
        { passenger_identity_key: uuidV4(), operator_trip_id: uuidV4() },
      ],
    }, { incentive: [150, 150, 150, 150, 150, 150, 0] }));
});

describe("Grand Châtellerault 2026 - Passenger daily limits", () => {
  it("should limit passenger daily trips to 2", async () =>
    await process({
      policy: { handler: Handler.id },
      carpool: [
        { driver_identity_key: uuidV4() },
        { driver_identity_key: uuidV4() },
        { driver_identity_key: uuidV4() },
      ],
    }, { incentive: [150, 150, 0] }));
});

describe("Grand Châtellerault 2026 - Driver monthly limits", () => {
  it("should limit driver monthly earnings to 120 €", async () => {
    await process({
      policy: { handler: Handler.id },
      carpool: [
        { distance: 6_000, driver_identity_key: "one" },
        { distance: 6_000, driver_identity_key: "one" },
      ],
      meta: [{
        key: "max_amount_restriction.0-one.month.1-2026",
        value: 118_50n,
      }, {
        key: "max_amount_restriction.global.campaign.global",
        value: 0n,
      }],
    }, {
      incentive: [150, 0],
      meta: [{
        key: "max_amount_restriction.0-one.month.1-2026",
        value: 120_00n,
      }, {
        key: "max_amount_restriction.global.campaign.global",
        value: 150n,
      }],
    });
  });
});

describe("Grand Châtellerault 2026 - Campaign global limit", () => {
  it("should limit campaign total amount to 15 000 €", async () => {
    await process({
      policy: { handler: Handler.id, max_amount: 15_000_00n },
      carpool: [{}, {}, {}],
      meta: [{
        key: "max_amount_restriction.global.campaign.global",
        value: 14_999_00n,
      }],
    }, {
      incentive: [100, 0, 0],
    });
  });
});
