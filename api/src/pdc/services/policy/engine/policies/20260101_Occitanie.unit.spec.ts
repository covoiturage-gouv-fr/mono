import { describe, it } from "@/dev_deps.ts";
import { v4 as uuidV4 } from "@/lib/uuid/index.ts";
import { CarpoolInterface, OperatorsEnum } from "../../interfaces/index.ts";
import { makeProcessHelper } from "../tests/macro.ts";
import { Occitanie20262027 as Handler } from "./20260101_Occitanie.ts";

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
      carpool: [{ operator_uuid: "not in list" }],
    }, { incentive: [0] }));

  it("distance below minimum", async () =>
    await process({
      policy: { handler: Handler.id },
      carpool: [{ distance: 1_999 }],
    }, { incentive: [0] }));

  it("distance above maximum", async () =>
    await process({
      policy: { handler: Handler.id },
      carpool: [
        { distance: 31_000 }, // regular start and end
        { distance: 31_000, start: { ...defaultCarpool.start, arr: "30294" } }, // inside specific area
        { distance: 51_000, start: { ...defaultCarpool.start, arr: "30294" } }, // inside specific area
      ],
    }, {
      incentive: [
        0,
        200,
        0,
      ],
    }));

  it("trip outside AOM", async () =>
    await process({
      policy: { handler: Handler.id },
      carpool: [
        { start: { ...defaultPosition, aom: "not_ok" }, end: { ...defaultPosition, aom: "not_ok" } },
        { start: { ...defaultPosition, aom: "not_ok" } },
        { end: { ...defaultPosition, aom: "not_ok" } },
      ],
    }, { incentive: [0, 0, 0] }));

  it("trip within same inner AOM", async () =>
    await process({
      policy: { handler: Handler.id },
      carpool: [{ start: { ...defaultPosition, aom: "253100986" }, end: { ...defaultPosition, aom: "253100986" } }],
    }, { incentive: [0] }));

  it("trip within different inner AOM (ok)", async () =>
    await process({
      policy: { handler: Handler.id },
      carpool: [
        // Grand-Cahors - CA du Pays de l'Or
        { start: { ...defaultPosition, aom: "200023737" }, end: { ...defaultPosition, aom: "243400470" } },
        // Occitanie - Grand-Cahors
        { start: { ...defaultPosition, aom: "200053791" }, end: { ...defaultPosition, aom: "243400470" } },
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

  it("sunday rides", async () =>
    await process({
      policy: { handler: Handler.id },
      carpool: [
        { datetime: new Date("2026-02-01") }, // Sunday
      ],
    }, { incentive: [0] }));

  it("trip outside of campaign dates", async () =>
    await process({
      policy: { handler: Handler.id },
      carpool: [{ datetime: new Date("2022-09-02") }],
    }, { incentive: [0] }));
});

describe("Occitanie 2026 - Incentives", () => {
  // De 2 à 20km : 2€ par passager transporté - contribution passager
  it("first slice (2-20)", async () =>
    await process({
      policy: { handler: Handler.id },
      carpool: [
        { distance: 2_000, passenger_identity_key: uuidV4(), passenger_contribution: 0 },
        { distance: 2_000, passenger_identity_key: uuidV4(), passenger_contribution: 200 },
        { distance: 10_000, passenger_identity_key: uuidV4(), passenger_contribution: 0 },
        { distance: 10_000, passenger_identity_key: uuidV4(), passenger_contribution: 100 },
        { distance: 15_000, passenger_identity_key: uuidV4(), passenger_contribution: 0 },
        { distance: 15_000, passenger_identity_key: uuidV4(), passenger_contribution: 50 },
      ],
    }, { incentive: [200, 0, 200, 100, 200, 150] }));

  // De 20 à 30km : 0,10 € par trajet par km par passager moins la contribution du passager, plafonné à 2,00 €
  it("second slice (20-30)", async () =>
    await process({
      policy: { handler: Handler.id },
      carpool: [
        { distance: 20_000, passenger_identity_key: uuidV4(), passenger_contribution: 0 },
        { distance: 20_000, passenger_identity_key: uuidV4(), passenger_contribution: 100 },
        { distance: 25_000, passenger_identity_key: uuidV4(), passenger_contribution: 0 },
        { distance: 25_000, passenger_identity_key: uuidV4(), passenger_contribution: 100 },
        { distance: 29_000, passenger_identity_key: uuidV4(), passenger_contribution: 0 },
        { distance: 29_000, passenger_identity_key: uuidV4(), passenger_contribution: 100 },
        { distance: 29_000, driver_identity_key: uuidV4(), passenger_contribution: 200 }, // switch driver identity to avoid daily limit
      ],
    }, { incentive: [200, 100, 200, 150, 200, 190, 90] }));

  // Pour les trajets au-delà de 30km et jusqu’à 50km sur les bassins listés en annexe 1 :
  // plafonné à 2€ par passager transporté
  it("third slice (30-50) specific areas", async () =>
    await process({
      policy: { handler: Handler.id },
      carpool: [
        { distance: 30_000 },
        { distance: 45_000 },
        { distance: 60_000 },
      ],
    }, { incentive: [250, 250, 250] }));

  it("farther away", async () =>
    await process({
      policy: { handler: Handler.id },
      carpool: [
        { distance: 80_000 },
        { distance: 100_000 },
      ],
    }, { incentive: [0, 0] }));
});

describe("Occitanie 2026 - Driver daily limits", () => {
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

describe("Occitanie 2026 - Driver monthly limits", () => {
  it("should limit driver monthly earnings to 150 €", async () => {
    await process({
      policy: { handler: Handler.id },
      carpool: [{ driver_identity_key: "driver_1" }],
      meta: [{
        key: "max_amount_restriction.0-driver_1.month.1-2026",
        value: 149_99n,
      }, {
        key: "max_amount_restriction.global.campaign.global",
        value: 149_99n,
      }],
    }, {
      incentive: [1], // only 0.01€ left to reach the limit
      meta: [{
        key: "max_amount_restriction.0-driver_1.month.1-2026",
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
        { driver_identity_key: "driver_1", datetime: new Date("2026-02-10") },
        { driver_identity_key: "driver_1", datetime: new Date("2026-03-10") },
      ],
      meta: [{
        key: "max_amount_restriction.0-driver_1.month.1-2026",
        value: 149_99n,
      }, {
        key: "max_amount_restriction.global.campaign.global",
        value: 149_99n,
      }],
    }, {
      incentive: [
        1, // only 0.01€ left to reach the limit
        1_00, // reset for new month : full incentive
      ],
      meta: [{
        key: "max_amount_restriction.0-driver_1.month.1-2026", // february
        value: 150_00n,
      }, {
        key: "max_amount_restriction.global.campaign.global",
        value: 151_00n,
      }, {
        key: "max_amount_restriction.0-driver_1.month.2-2026", // march
        value: 1_00n,
      }],
    });
  });
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
      incentive: [100, 0, 0], // only 1.00€ left to reach the limit
    });
  });
});
