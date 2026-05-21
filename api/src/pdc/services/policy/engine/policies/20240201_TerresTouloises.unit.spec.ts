import { it } from "dep:testing-bdd";
import { v4 as uuidV4 } from "../../../../../lib/uuid/index.ts";
import { OperatorsEnum } from "../../interfaces/index.ts";
import { makeProcessHelper } from "../tests/macro.ts";
import { TerresTouloises2024 as Handler } from "./20240201_TerresTouloises.ts";

// Toul
const defaultPosition = {
  arr: "54528",
  com: "54528",
  aom: "200070563",
  epci: "200070563",
  dep: "54",
  reg: "44",
  country: "XXXXX",
};
const defaultLat = 48.67590006663716;
const defaultLon = 5.890172847442269;

const defaultCarpool = {
  _id: 1,
  operator_trip_id: uuidV4(),
  passenger_identity_key: uuidV4(),
  driver_identity_key: uuidV4(),
  operator_uuid: OperatorsEnum.MOBICOOP,
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

it(
  "should work with exclusions",
  async () =>
    await process(
      {
        policy: { handler: Handler.id },
        carpool: [
          { distance: 4_999 },
          { distance: 80_001 },
          { operator_class: "A" },
          { operator_uuid: OperatorsEnum.KAROS },

          // // OD hors AOM
          {
            start: { ...defaultPosition, aom: "244900015" },
            end: { ...defaultPosition, aom: "244900015" },
          },

          // O hors AOM et D dans l'AOM
          {
            start: { ...defaultPosition, aom: "200071678" },
            end: { ...defaultPosition, aom: "200070563" },
          },

          { passenger_is_over_18: false },
        ],
        meta: [],
      },
      { incentive: [0, 0, 0, 0, 0, 0, 0], meta: [] },
    ),
);

it(
  "should work basic",
  async () =>
    await process(
      {
        policy: { handler: Handler.id },
        carpool: [
          {
            distance: 1_000,
            driver_identity_key: uuidV4(),
            passenger_identity_key: uuidV4(),
          },
          {
            distance: 5_000,
            driver_identity_key: uuidV4(),
            passenger_identity_key: uuidV4(),
          },
          {
            distance: 5_000,
            driver_identity_key: uuidV4(),
            passenger_identity_key: uuidV4(),
            seats: 2,
          },
          {
            distance: 14_000,
            driver_identity_key: uuidV4(),
            passenger_identity_key: uuidV4(),
          },
          {
            distance: 15_000,
            driver_identity_key: uuidV4(),
            passenger_identity_key: uuidV4(),
          },
          {
            distance: 80_000,
            driver_identity_key: uuidV4(),
            passenger_identity_key: uuidV4(),
          },
        ],
        meta: [],
      },
      {
        incentive: [0, 200, 300, 250, 300, 0],
      },
    ),
);

it(
  "should work with global limits",
  async () =>
    await process(
      {
        policy: { handler: Handler.id, max_amount: 2_200_000_00 },
        carpool: [{ distance: 5_000, driver_identity_key: "one" }],
        meta: [
          {
            key: "max_amount_restriction.global.campaign.global",
            value: 2_199_998_00,
          },
        ],
      },
      {
        incentive: [200],
        meta: [
          {
            key: "max_amount_restriction.global.campaign.global",
            value: 2_200_000_00,
          },
          {
            key: "max_amount_restriction.0-one.month.3-2024",
            value: 200,
          },
        ],
      },
    ),
);

it(
  "should work with day limits",
  async () =>
    await process(
      {
        policy: { handler: Handler.id },
        carpool: [
          { distance: 5_000, driver_identity_key: "driver_A" },
          { distance: 5_000, driver_identity_key: "driver_A" },
          { distance: 5_000, driver_identity_key: "driver_A" },
          {
            distance: 5_000,
            driver_identity_key: "driver_B",
            passenger_identity_key: "pass_A",
          },
          {
            distance: 5_000,
            driver_identity_key: "driver_C",
            passenger_identity_key: "pass_A",
          },
          {
            distance: 5_000,
            driver_identity_key: "driver_D",
            passenger_identity_key: "pass_A",
          },
        ],
        meta: [],
      },
      {
        incentive: [200, 200, 0, 200, 200, 0],
        meta: [
          {
            key: "max_amount_restriction.global.campaign.global",
            value: 800,
          },
          {
            key: "max_amount_restriction.0-driver_A.month.3-2024",
            value: 400,
          },
          {
            key: "max_amount_restriction.0-driver_B.month.3-2024",
            value: 200,
          },
          {
            key: "max_amount_restriction.0-driver_C.month.3-2024",
            value: 200,
          },
          {
            key: "max_amount_restriction.0-driver_D.month.3-2024",
            value: 0,
          },
        ],
      },
    ),
);

it(
  "should work with driver month limits of 100 €",
  async () =>
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
            value: 100_00,
          },
          {
            key: "max_amount_restriction.0-one.month.3-2024",
            value: 98_00,
          },
        ],
      },
      {
        incentive: [200, 0],
        meta: [
          {
            key: "max_amount_restriction.global.campaign.global",
            value: 102_00,
          },
          {
            key: "max_amount_restriction.0-one.month.3-2024",
            value: 100_00,
          },
        ],
      },
    ),
);
