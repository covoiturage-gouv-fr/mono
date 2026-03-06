import { describe, it } from "@/dev_deps.ts";
import { v4 as uuidV4 } from "@/lib/uuid/index.ts";
import { CarpoolInterface, OperatorsEnum, TerritoryCodeEnum } from "../../interfaces/index.ts";
import { makeProcessHelper } from "../tests/macro.ts";
import { Occitanie20262027 as Handler } from "./20260101_Occitanie.ts";
import { occitanie2026Data } from "./20260101_Occitanie.data.ts";

const defaultPosition = {
  arr: "46240",
  com: "46240",
  aom: "200053791",
  epci: "200066371",
  dep: "46",
  reg: "76",
  country: "XXXXX",
  reseau: 465,
};
const defaultLat = 48.72565703413325;
const defaultLon = 2.261827843187402;

const defaultCarpool: CarpoolInterface = {
  operator_trip_id: uuidV4(),
  passenger_identity_key: uuidV4(),
  driver_identity_key: uuidV4(),
  operator_uuid: OperatorsEnum.BLABLACAR_DAILY,
  operator_class: "C",
  passenger_is_over_18: true,
  passenger_has_travel_pass: true,
  driver_has_travel_pass: true,
  datetime: new Date("2026-02-02"), // Monday
  seats: 1,
  distance: 5_000,
  operator_journey_id: uuidV4(),
  operator_id: 1,
  start: { ...defaultPosition },
  end: { ...defaultPosition },
  start_lat: defaultLat,
  start_lon: defaultLon,
  end_lat: defaultLat,
  end_lon: defaultLon,
  driver_revenue: 10,
  passenger_contribution: 50, // minimum 0.5€
};

const process = makeProcessHelper(defaultCarpool);

describe("Occitanie 2026 - Exclusions", () => {
  it("unsupported operator", async () =>
    await process({
      policy: { handler: Handler.id },
      carpool: [
        // Non listed operator will fail
        { operator_uuid: OperatorsEnum.BLABLACAR, datetime: new Date("2026-02-02") },

        // Supported operator but before starting date
        { operator_uuid: OperatorsEnum.BLABLACAR_DAILY, datetime: new Date("2025-02-02") },
      ],
    }, { incentive: [0, 0] }));

  it("distance below minimum", async () =>
    await process({
      policy: { handler: Handler.id },
      carpool: [{ distance: 1_999 }],
    }, { incentive: [0] }));

  // The campaign has different maximum distances based on areas
  it("distance above maximum", async () =>
    await process({
      policy: { handler: Handler.id },
      carpool: [
        // regular area : 30 km max
        { distance: 31_000 },

        // inside specific area : 50 km max
        { distance: 31_000, start: { ...defaultCarpool.start, arr: occitanie2026Data.areas[0] } }, // ok
        { distance: 51_000, start: { ...defaultCarpool.start, arr: occitanie2026Data.areas[0] } }, // ko
      ],
    }, {
      incentive: [0, 200, 0],
    }));

  // trips must start AND end within Occitanie region
  it("trip outside AOM", async () =>
    await process({
      policy: { handler: Handler.id },
      carpool: [
        { start: { ...defaultPosition, aom: "not_ok" }, end: { ...defaultPosition, aom: "not_ok" } },
        { start: { ...defaultPosition, aom: "not_ok" } },
        { end: { ...defaultPosition, aom: "not_ok" } },
      ],
    }, { incentive: [0, 0, 0] }));

  // Trips from within the same inner AOM are rejected
  it("trip within same inner AOM", async () =>
    await process({
      policy: { handler: Handler.id },
      carpool: [{
        start: { ...defaultPosition, aom: occitanie2026Data.inner[0] },
        end: { ...defaultPosition, aom: occitanie2026Data.inner[0] },
      }],
    }, { incentive: [0] }));

  it("trip within different inner AOM (ok)", async () =>
    await process({
      policy: { handler: Handler.id },
      carpool: [
        // Grand-Cahors - CA Grand Montauban
        {
          start: { ...defaultPosition, aom: occitanie2026Data.innerByName("CA du Grand Cahors") },
          end: { ...defaultPosition, aom: occitanie2026Data.innerByName("CA Grand Montauban") },
        },
        // Occitanie - Grand-Cahors
        {
          start: { ...defaultPosition, aom: occitanie2026Data.region[TerritoryCodeEnum.Mobility] },
          end: { ...defaultPosition, aom: occitanie2026Data.innerByName("CA du Grand Cahors") },
        },
      ],
    }, { incentive: [150, 150] }));

  it("trip outside the region", async () =>
    await process({
      policy: { handler: Handler.id },
      carpool: [{ start: { ...defaultPosition, reg: "84" } }],
    }, { incentive: [0] }));

  it("operator class not eligible", async () =>
    await process({
      policy: { handler: Handler.id },
      carpool: [{ operator_class: "A" }],
    }, { incentive: [0] }));

  it("passenger contribution too low", async () =>
    await process({
      policy: { handler: Handler.id },
      carpool: [
        { passenger_contribution: 0 },
        { passenger_contribution: 49 },
      ],
    }, { incentive: [0, 0] }));

  it("trip outside of campaign dates", async () =>
    await process({
      policy: { handler: Handler.id },
      carpool: [{ datetime: new Date("2022-09-02") }],
    }, { incentive: [0] }));
});

describe("Occitanie 2027 - Incentives", () => {
  // De 2 à 20km : 2€ par passager transporté - contribution passager
  it("first slice (2-20)", async () =>
    await process({
      policy: { handler: Handler.id },
      carpool: [
        { distance: 2_000, passenger_identity_key: uuidV4(), passenger_contribution: 100 },
        { distance: 2_000, passenger_identity_key: uuidV4(), passenger_contribution: 200 },
        { distance: 10_000, passenger_identity_key: uuidV4(), passenger_contribution: 100 },
        { distance: 10_000, passenger_identity_key: uuidV4(), passenger_contribution: 50 },
        { distance: 15_000, passenger_identity_key: uuidV4(), passenger_contribution: 100 },
        { distance: 15_000, passenger_identity_key: uuidV4(), passenger_contribution: 250 }, // possible ?
      ],
    }, { incentive: [100, 0, 100, 150, 100, 0] }));

  // De 20 à 30km : 0,10 € par trajet par km par passager moins la contribution du passager, plafonné à 2,00 €
  it("second slice (20-30)", async () =>
    await process({
      policy: { handler: Handler.id },
      carpool: [
        // 20 x 0,10 - 0,50 = 1,50 €
        { distance: 20_000, passenger_identity_key: uuidV4(), passenger_contribution: 50 },

        // 20 x 0,10 - 1,00 = 1,00 €
        { distance: 20_000, passenger_identity_key: uuidV4(), passenger_contribution: 100 },

        // 25 * 0,10 - 0,50 = 2,00 €
        { distance: 25_000, passenger_identity_key: uuidV4(), passenger_contribution: 50 },

        // 25 * 0,10 - 1,00 = 1,50 €
        { distance: 25_000, passenger_identity_key: uuidV4(), passenger_contribution: 100 },

        // 29 * 0,10 - 0,50 = 2,40 € -> capped at 2,00 €
        { distance: 29_000, passenger_identity_key: uuidV4(), passenger_contribution: 50 },

        // 29 * 0,10 - 1,00 = 1,90 €
        { distance: 29_000, passenger_identity_key: uuidV4(), passenger_contribution: 100 },

        // 29 * 0,10 - 2,00 = 0,90 €
        { distance: 29_000, driver_identity_key: uuidV4(), passenger_contribution: 200 }, // switch driver identity to avoid daily limit
      ],
    }, { incentive: [150, 100, 200, 150, 200, 190, 90] }));

  // Pour les trajets au-delà de 30km et jusqu’à 50km sur les bassins listés en annexe 1 :
  // plafonné à 2€ par passager transporté
  it("third slice (30-50) specific areas", async () =>
    await process({
      policy: { handler: Handler.id },
      carpool: [
        { distance: 30_000, start: { ...defaultCarpool.start, arr: occitanie2026Data.areas[0] } },
        { distance: 49_999, start: { ...defaultCarpool.start, arr: occitanie2026Data.areas[0] } },
      ],
    }, { incentive: [200, 200] }));

  it("farther away", async () =>
    await process({
      policy: { handler: Handler.id },
      carpool: [
        { distance: 30_000 },
        { distance: 50_000, start: { ...defaultCarpool.start, arr: occitanie2026Data.areas[0] } },
      ],
    }, { incentive: [0, 0] }));
});

describe("Occitanie 2026 - Driver daily limits", () => {
  it("should limit driver daily trips to 6", async () =>
    await process({
      policy: { handler: Handler.id },
      carpool: [
        { operator_trip_id: uuidV4(), passenger_identity_key: uuidV4() },
        { operator_trip_id: uuidV4(), passenger_identity_key: uuidV4() },
        { operator_trip_id: uuidV4(), passenger_identity_key: uuidV4() },
        { operator_trip_id: uuidV4(), passenger_identity_key: uuidV4() },
        { operator_trip_id: uuidV4(), passenger_identity_key: uuidV4() },
        { operator_trip_id: uuidV4(), passenger_identity_key: uuidV4() },
        { operator_trip_id: uuidV4(), passenger_identity_key: uuidV4() }, /* exceeds driver limit here */
      ],
    }, { incentive: [150, 150, 150, 150, 150, 150, 0] }));

  it("should reset driver limits on different days", async () =>
    await process({
      policy: { handler: Handler.id },
      carpool: [
        { datetime: new Date("2026-02-02T10:00:00"), passenger_identity_key: uuidV4() },
        { datetime: new Date("2026-02-02T12:00:00"), passenger_identity_key: uuidV4() },
        { datetime: new Date("2026-02-02T14:00:00"), passenger_identity_key: uuidV4() },
        { datetime: new Date("2026-02-02T16:00:00"), passenger_identity_key: uuidV4() },
        { datetime: new Date("2026-02-02T16:00:00"), passenger_identity_key: uuidV4() },
        { datetime: new Date("2026-02-02T16:00:00"), passenger_identity_key: uuidV4() },
        { datetime: new Date("2026-02-02T18:00:00"), passenger_identity_key: uuidV4() }, // exceeds driver limit here

        // next day
        { datetime: new Date("2026-02-03T10:00:00"), passenger_identity_key: uuidV4() },
        { datetime: new Date("2026-02-03T12:00:00"), passenger_identity_key: uuidV4() },
      ],
    }, { incentive: [150, 150, 150, 150, 150, 150, 0, 150, 150] }));
});

describe("Occitanie 2026 - Passenger daily limits", () => {
  it("should limit passenger daily trips to 2", async () =>
    await process({
      policy: { handler: Handler.id },
      carpool: [
        { operator_trip_id: uuidV4(), driver_identity_key: uuidV4() },
        { operator_trip_id: uuidV4(), driver_identity_key: uuidV4() },
        { operator_trip_id: uuidV4(), driver_identity_key: uuidV4() }, /* exceeds driver limit here */
      ],
    }, { incentive: [150, 150, 0] }));

  it("should reset passenger limits on different days", async () =>
    await process({
      policy: { handler: Handler.id },
      carpool: [
        { datetime: new Date("2026-02-02T10:00:00"), driver_identity_key: uuidV4() },
        { datetime: new Date("2026-02-02T12:00:00"), driver_identity_key: uuidV4() },
        { datetime: new Date("2026-02-02T18:00:00"), driver_identity_key: uuidV4() }, // exceeds driver limit here

        // next day
        { datetime: new Date("2026-02-03T10:00:00"), driver_identity_key: uuidV4() },
        { datetime: new Date("2026-02-03T12:00:00"), driver_identity_key: uuidV4() },
      ],
    }, { incentive: [150, 150, 0, 150, 150] }));
});

describe("Occitanie 2026 - Campaign global limit", () => {
  it("should limit campaign total amount to 210 000 €", async () => {
    await process({
      policy: { handler: Handler.id, max_amount: 61_163_00n },
      carpool: [{}, {}, {}],
      meta: [{
        key: "max_amount_restriction.global.campaign.global",
        value: 61_162_00n,
      }],
    }, {
      incentive: [100, 0, 0], // 1.00€ left to reach the limit
    });
  });
});
