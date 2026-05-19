import { describe, it } from "dep:testing-bdd";
import { v4 as uuidV4 } from "@/lib/uuid/index.ts";
import { OperatorsEnum } from "../../interfaces/index.ts";
import { makeProcessHelper } from "../tests/macro.ts";
import { CCSPSL2026 as Handler } from "./20260301_CCSPSL_2026.ts";

// Saint-Pourcain-sur-Sioule (03254), EPCI 200071389
const defaultPosition = {
  arr: "03254",
  com: "03254",
  aom: "200071389",
  epci: "200071389",
  dep: "03",
  reg: "84",
  country: "XXXXX",
  reseau: 1,
};
const defaultLat = 46.3108;
const defaultLon = 3.2897;

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
  datetime: new Date("2026-04-01"),
  seats: 1,
  distance: 5_000,
  operator_journey_id: uuidV4(),
  operator_id: 1,
  driver_revenue: 1_00,
  passenger_contribution: 0,
  start: { ...defaultPosition },
  end: { ...defaultPosition },
  start_lat: defaultLat,
  start_lon: defaultLon,
  end_lat: defaultLat,
  end_lon: defaultLon,
};

const process = makeProcessHelper(defaultCarpool);

describe("CCSPSL 2026 - Exclusions", () => {
  it("unsupported operator", async () =>
    await process({
      policy: { handler: Handler.id },
      carpool: [{ operator_uuid: "not_in_list" }],
    }, { incentive: [0] }));

  it("distance below minimum", async () =>
    await process({
      policy: { handler: Handler.id },
      carpool: [{ distance: 1_999 }],
    }, { incentive: [0] }));

  it("distance above maximum", async () =>
    await process({
      policy: { handler: Handler.id },
      carpool: [{ distance: 80_001 }],
    }, { incentive: [0] }));

  it("trip outside EPCI", async () =>
    await process({
      policy: { handler: Handler.id },
      carpool: [{
        start: { ...defaultPosition, epci: "not_ok" },
        end: { ...defaultPosition, epci: "not_ok" },
      }],
    }, { incentive: [0] }));

  it("trip with start in EPCI is eligible", async () =>
    await process({
      policy: { handler: Handler.id },
      carpool: [{
        start: { ...defaultPosition },
        end: { ...defaultPosition, epci: "not_ok" },
      }],
    }, { incentive: [100] }));

  it("trip with end in EPCI is eligible", async () =>
    await process({
      policy: { handler: Handler.id },
      carpool: [{
        start: { ...defaultPosition, epci: "not_ok" },
        end: { ...defaultPosition },
      }],
    }, { incentive: [100] }));

  it("operator class not eligible", async () =>
    await process({
      policy: { handler: Handler.id },
      carpool: [{ operator_class: "A" }],
    }, { incentive: [0] }));

  it("trip outside campaign dates", async () =>
    await process({
      policy: { handler: Handler.id },
      carpool: [{ datetime: new Date("2025-01-01") }],
    }, { incentive: [0] }));
});

describe("CCSPSL 2026 - Incentives par tranche de distance", () => {
  // [2, 15) km : 1.00 EUR par passager
  it("tranche 2-15 km", async () =>
    await process({
      policy: { handler: Handler.id },
      carpool: [
        { distance: 2_000 },
        { distance: 5_000 },
        { distance: 10_000 },
        { distance: 14_999 },
      ],
    }, { incentive: [100, 100, 100, 100] }));

  // [15, 30) km : 1.00 EUR + 0.10 EUR/km au-dela de 15 km
  it("tranche 15-30 km", async () =>
    await process({
      policy: { handler: Handler.id },
      carpool: [
        { distance: 15_000 }, // 100 + 10*0 = 100
        { distance: 20_000 }, // 100 + 10*5 = 150
        { distance: 25_000 }, // 100 + 10*10 = 200
        { distance: 29_000 }, // 100 + 10*14 = 240
      ],
    }, { incentive: [100, 150, 200, 240] }));

  // [30, 60) km : 2.00 EUR par passager
  it("tranche 30-60 km", async () =>
    await process({
      policy: { handler: Handler.id },
      carpool: [
        { distance: 30_000 },
        { distance: 45_000 },
        { distance: 59_999 },
      ],
    }, { incentive: [200, 200, 200] }));

  // [60, 80] km : 1.50 EUR par passager
  it("tranche 60-80 km", async () =>
    await process({
      policy: { handler: Handler.id },
      carpool: [
        { distance: 60_000 },
        { distance: 70_000 },
        { distance: 80_000 },
      ],
    }, { incentive: [150, 150, 150] }));

  it("multi-seat multiplier", async () =>
    await process({
      policy: { handler: Handler.id },
      carpool: [
        { distance: 5_000, seats: 2 },  // 100 * 2 = 200
        { distance: 20_000, seats: 3 }, // 150 * 3 = 450
        { distance: 45_000, seats: 2 }, // 200 * 2 = 400
        { distance: 65_000, seats: 2 }, // 150 * 2 = 300
      ],
    }, { incentive: [200, 450, 400, 300] }));
});

describe("CCSPSL 2026 - Limite journaliere conducteur", () => {
  it("should limit to 6 journeys per driver per day", async () =>
    await process({
      policy: { handler: Handler.id },
      carpool: [
        { operator_trip_id: uuidV4() },
        { operator_trip_id: uuidV4() },
        { operator_trip_id: uuidV4() },
        { operator_trip_id: uuidV4() },
        { operator_trip_id: uuidV4() },
        { operator_trip_id: uuidV4() },
        { operator_trip_id: uuidV4() }, // 7th -> excluded
      ],
    }, { incentive: [100, 100, 100, 100, 100, 100, 0] }));
});

describe("CCSPSL 2026 - Limite mensuelle conducteur", () => {
  it("should cap driver monthly earnings at 150 EUR", async () => {
    await process({
      policy: { handler: Handler.id },
      carpool: [{ driver_identity_key: "driver_1" }],
      meta: [{
        key: "max_amount_restriction.0-driver_1.month.3-2026",
        value: 149_00n,
      }, {
        key: "max_amount_restriction.global.campaign.global",
        value: 149_00n,
      }],
    }, {
      incentive: [1_00], // only 1 EUR left
      meta: [{
        key: "max_amount_restriction.0-driver_1.month.3-2026",
        value: 150_00n,
      }, {
        key: "max_amount_restriction.global.campaign.global",
        value: 150_00n,
      }],
    });
  });

  it("should reset limits on next month", async () => {
    await process({
      policy: { handler: Handler.id },
      carpool: [
        { driver_identity_key: "driver_1", datetime: new Date("2026-04-10") },
        { driver_identity_key: "driver_1", datetime: new Date("2026-05-10") },
      ],
      meta: [{
        key: "max_amount_restriction.0-driver_1.month.3-2026",
        value: 149_00n,
      }, {
        key: "max_amount_restriction.global.campaign.global",
        value: 149_00n,
      }],
    }, {
      incentive: [
        1_00, // only 1 EUR left in April
        1_00, // reset for May: full incentive
      ],
      meta: [{
        key: "max_amount_restriction.0-driver_1.month.3-2026",
        value: 150_00n,
      }, {
        key: "max_amount_restriction.global.campaign.global",
        value: 151_00n,
      }, {
        key: "max_amount_restriction.0-driver_1.month.4-2026",
        value: 1_00n,
      }],
    });
  });
});

describe("CCSPSL 2026 - Plafond global de la campagne", () => {
  it("should limit total campaign amount to 2 135 EUR", async () => {
    await process({
      policy: { handler: Handler.id, max_amount: 2_135_00n },
      carpool: [{}, {}, {}],
      meta: [{
        key: "max_amount_restriction.global.campaign.global",
        value: 2_134_50n,
      }],
    }, {
      incentive: [50, 0, 0], // only 0.50 EUR left
    });
  });
});
