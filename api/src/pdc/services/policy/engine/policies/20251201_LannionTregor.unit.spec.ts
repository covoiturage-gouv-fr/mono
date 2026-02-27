import { describe, it } from "@/dev_deps.ts";
import { v4 as uuidV4 } from "@/lib/uuid/index.ts";
import { CarpoolInterface, OperatorsEnum } from "../../interfaces/index.ts";
import { makeProcessHelper } from "../tests/macro.ts";
import { LannionTregor2026 as Handler } from "./20251201_LannionTregor.ts";

// Perros-Guirec
const defaultPosition = {
  arr: "22168",
  com: "22168",
  aom: "200065928",
  epci: "200065928",
  dep: "22",
  reg: "53",
  country: "XXXXX",
  reseau: 194,
};

const defaultLat = 48.81387693883991;
const defaultLon = -3.4424441671291306;

const defaultCarpool: CarpoolInterface = {
  passenger_contribution: 150,
  passenger_identity_key: uuidV4(),
  passenger_has_travel_pass: true,
  passenger_is_over_18: true,
  driver_revenue: 150,
  driver_identity_key: uuidV4(),
  driver_has_travel_pass: true,
  operator_journey_id: uuidV4(),
  operator_id: 9,
  operator_trip_id: uuidV4(),
  operator_uuid: OperatorsEnum.BLABLACAR_DAILY,
  operator_class: "C",
  datetime: new Date("2026-02-01"),
  seats: 1,
  distance: 5_000,
  start: { ...defaultPosition },
  start_lat: defaultLat,
  start_lon: defaultLon,
  end: { ...defaultPosition },
  end_lat: defaultLat,
  end_lon: defaultLon,
};

const process = makeProcessHelper(defaultCarpool);

describe("Lannion Tregor 2026 - Exclusions", () => {
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

describe("Lannion Tregor 2026 - Incentives", () => {
  it("first slice (2-15)", async () =>
    await process({
      policy: { handler: Handler.id },
      carpool: [
        { distance: 2_000 },
        { distance: 10_000 },
        { distance: 15_000 },
      ],
    }, { incentive: [100, 100, 100] }));

  it("second slice (15-30)", async () =>
    await process({
      policy: { handler: Handler.id },
      carpool: [
        { distance: 15_000 },
        { distance: 20_000 },
        { distance: 30_000 },
      ],
    }, { incentive: [100, 150, 250] }));

  it("third slice (30-60)", async () =>
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

describe("Lannion Tregor 2026 - Driver daily limits", () => {
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

describe("Lannion Tregor 2026 - Driver monthly limits", () => {
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

describe("Lannion Tregor 2026 - Campaign global limit", () => {
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
