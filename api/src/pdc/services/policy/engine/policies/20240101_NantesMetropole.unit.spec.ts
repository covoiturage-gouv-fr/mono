import { it } from "@/dev_deps.ts";
import { v4 as uuidV4 } from "@/lib/uuid/index.ts";
import sinon from "dev:sinon";
import { OperatorsEnum } from "../../interfaces/index.ts";
import { dateRange } from "../helpers/dateRange.ts";
import { makeProcessHelper } from "../tests/macro.ts";
import { NantesMetropole2024 as Handler } from "./20240101_NantesMetropole.ts";

sinon.stub(Handler, "boosterDates").get(() => [...dateRange("2024-12-01", "2024-12-31")]);

const defaultPosition = {
  arr: "44109",
  com: "44109",
  aom: "244400404",
  epci: "244400404",
  dep: "44",
  reg: "52",
  country: "XXXXX",
  network: 44,
};
const defaultLat = 48.72565703413325;
const defaultLon = 2.261827843187402;

const defaultCarpool = {
  _id: 1,
  operator_trip_id: uuidV4(),
  passenger_identity_key: uuidV4(),
  driver_identity_key: uuidV4(),
  operator_uuid: OperatorsEnum.KAROS,
  operator_class: "C",
  passenger_is_over_18: true,
  passenger_has_travel_pass: true,
  driver_has_travel_pass: true,
  datetime: new Date("2024-04-15"),
  seats: 1,
  distance: 5_000,
  operator_journey_id: uuidV4(),
  operator_id: 1,
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

it("should work with exclusions", async () =>
  await process(
    {
      policy: { handler: Handler.id },
      carpool: [
        { distance: 4_999 },
        { distance: 60_001 },
        { operator_class: "A" },

        // // OD hors AOM
        {
          start: { ...defaultPosition, aom: "244900015" },
          end: { ...defaultPosition, aom: "244900015" },
        },

        // O dans l'AOM et D hors AOM
        {
          start: { ...defaultPosition, aom: "244400404" },
          end: { ...defaultPosition, aom: "247200132" },
        },

        // O hors AOM et D dans l'AOM
        {
          start: { ...defaultPosition, aom: "200071678" },
          end: { ...defaultPosition, aom: "244400404" },
        },

        // // Région Île-de-France
        { start: { ...defaultPosition, reg: "11" } },
        { end: { ...defaultPosition, reg: "11" } },
        { passenger_is_over_18: false },
      ],
    },
    { incentive: [0, 0, 0, 0, 0, 0, 0, 0, 0] },
  ));

it("should work basic", async () =>
  await process(
    {
      policy: { handler: Handler.id },
      carpool: [
        { distance: 1_000, driver_identity_key: "one" },
        { distance: 5_000, driver_identity_key: "one" },
        { distance: 5_000, seats: 2, driver_identity_key: "one" },
        { distance: 20_000, driver_identity_key: "two" },
        { distance: 25_000, driver_identity_key: "two" },
        { distance: 29_500, driver_identity_key: "two" },
        { distance: 30_000, driver_identity_key: "two" },
        { distance: 55_000, driver_identity_key: "two" },
        { distance: 61_000, driver_identity_key: "two" },
      ],
      meta: [],
    },
    {
      incentive: [0, 75, 150, 105, 155, 200, 200, 200, 0],
      meta: [
        {
          key: "max_amount_restriction.global.campaign.global",
          value: 1085n,
        },
        {
          key: "max_amount_restriction.0-one.month.3-2024",
          value: 225n,
        },
        {
          key: "max_amount_restriction.0-one.year.2024",
          value: 225n,
        },
        {
          key: "max_amount_restriction.0-two.month.3-2024",
          value: 860n,
        },
        {
          key: "max_amount_restriction.0-two.year.2024",
          value: 860n,
        },
      ],
    },
  ));

it("should work with global limits", async () =>
  await process(
    {
      policy: { handler: Handler.id, max_amount: 2_200_000_00n },
      carpool: [{ distance: 5_000, driver_identity_key: "one" }],
      meta: [
        {
          key: "max_amount_restriction.global.campaign.global",
          value: 2_199_999_25n,
        },
      ],
    },
    {
      incentive: [75],
      meta: [
        {
          key: "max_amount_restriction.global.campaign.global",
          value: 2_200_000_00n,
        },
        {
          key: "max_amount_restriction.0-one.month.3-2024",
          value: 75n,
        },
        {
          key: "max_amount_restriction.0-one.year.2024",
          value: 75n,
        },
      ],
    },
  ));

it("should work with day limits", async () =>
  await process(
    {
      policy: { handler: Handler.id },
      carpool: [
        { distance: 5_000, driver_identity_key: "one" },
        { distance: 5_000, driver_identity_key: "one" },
        { distance: 5_000, driver_identity_key: "one" },
        { distance: 5_000, driver_identity_key: "one" },
        { distance: 5_000, driver_identity_key: "one" },
        { distance: 5_000, driver_identity_key: "one" },
        { distance: 5_000, driver_identity_key: "one" },
      ],
      meta: [],
    },
    {
      incentive: [75, 75, 75, 75, 75, 75, 0],
      meta: [
        {
          key: "max_amount_restriction.global.campaign.global",
          value: 450n,
        },
        {
          key: "max_amount_restriction.0-one.month.3-2024",
          value: 450n,
        },
        {
          key: "max_amount_restriction.0-one.year.2024",
          value: 450n,
        },
      ],
    },
  ));

it("should work with driver month limits of 84 €", async () =>
  await process(
    {
      policy: { handler: Handler.id },
      carpool: [
        { distance: 6_000, driver_identity_key: "one" },
        { distance: 6_000, driver_identity_key: "one" },
      ],
      meta: [
        {
          key: "max_amount_restriction.global.campaign.global",
          value: 100_00n,
        },
        {
          key: "max_amount_restriction.0-one.month.3-2024",
          value: 83_25n,
        },
        {
          key: "max_amount_restriction.0-one.year.2024",
          value: 83_25n,
        },
      ],
    },
    {
      incentive: [75, 0],
      meta: [
        {
          key: "max_amount_restriction.global.campaign.global",
          value: 100_75n,
        },
        {
          key: "max_amount_restriction.0-one.month.3-2024",
          value: 84_00n,
        },
        {
          key: "max_amount_restriction.0-one.year.2024",
          value: 84_75n,
        },
      ],
    },
  ));

it("should work with driver year limits of 1008.00 €", async () =>
  await process(
    {
      policy: { handler: Handler.id },
      carpool: [
        { distance: 6_000, driver_identity_key: "one" },
        { distance: 6_000, driver_identity_key: "one" },
      ],
      meta: [
        {
          key: "max_amount_restriction.global.campaign.global",
          value: 100_00n,
        },
        {
          key: "max_amount_restriction.0-one.month.3-2024",
          value: 0n,
        },
        {
          key: "max_amount_restriction.0-one.year.2024",
          value: 1007_25n,
        },
      ],
    },
    {
      incentive: [75, 0],
      meta: [
        {
          key: "max_amount_restriction.global.campaign.global",
          value: 100_75n,
        },
        {
          key: "max_amount_restriction.0-one.month.3-2024",
          value: 75n,
        },
        {
          key: "max_amount_restriction.0-one.year.2024",
          value: 1008_00n,
        },
      ],
    },
  ));

it("should use boosterSlices on booster dates", async () =>
  await process(
    {
      policy: { handler: Handler.id },
      carpool: [
        {
          distance: 6_000,
          driver_identity_key: "reg",
          datetime: new Date("2024-04-15"),
        },
        {
          distance: 6_000,
          driver_identity_key: "boo",
          datetime: new Date("2024-12-01"),
        },
        {
          distance: 11_000,
          driver_identity_key: "reg",
          datetime: new Date("2024-04-15"),
        },
        {
          distance: 11_000,
          driver_identity_key: "boo",
          datetime: new Date("2024-12-01"),
        },
        {
          distance: 17_000,
          driver_identity_key: "reg",
          datetime: new Date("2024-04-15"),
        },
        {
          distance: 17_000,
          driver_identity_key: "boo",
          datetime: new Date("2024-12-01"),
        },
        {
          distance: 30_000,
          driver_identity_key: "reg",
          datetime: new Date("2024-04-15"),
        },
        {
          distance: 30_000,
          driver_identity_key: "boo",
          datetime: new Date("2024-12-01"),
        },
        {
          distance: 75_000,
          driver_identity_key: "reg",
          datetime: new Date("2024-04-15"),
        },
        {
          distance: 75_000,
          driver_identity_key: "boo",
          datetime: new Date("2024-12-01"),
        },
      ],
    },
    {
      incentive: [75, 165, 75, 165, 75, 165, 200, 290, 0, 0],
    },
  ));

it("should detect trips inside the AOM", async () =>
  await process(
    {
      policy: { handler: Handler.id },
      carpool: [
        // regular - inside
        { distance: 6_000, driver_identity_key: "one", datetime: new Date("2024-04-15") },

        // // booster - inside
        { distance: 6_000, driver_identity_key: "one", datetime: new Date("2024-12-01") },

        // regular - outside
        {
          distance: 6_000,
          datetime: new Date("2024-04-15"),
          start: { ...defaultPosition, aom: "244900015" }, // Angers
        },

        // booster - outside
        {
          distance: 6_000,
          datetime: new Date("2024-12-04"),
          start: { ...defaultPosition, aom: "244900015" }, // Angers
          seats: 1,
        },

        // booster - outside - many seats
        {
          distance: 6_000,
          driver_identity_key: "one",
          datetime: new Date("2024-12-12"),
          start: { ...defaultPosition, aom: "244900015" }, // Angers
          seats: 3,
        },

        // booster - outside - slice 2
        {
          distance: 20_000,
          datetime: new Date("2024-12-25"),
          start: { ...defaultPosition, aom: "244900015" }, // Angers
        },

        // booster - outside - slice 3
        {
          distance: 40_000,
          datetime: new Date("2024-12-10"),
          start: { ...defaultPosition, aom: "244900015" }, // Angers
        },
      ],
    },
    {
      incentive: [75, 165, 0, 90, 90 * 3, 90, 90],
    },
  ));
