import { it } from "dep:testing-bdd";
import { v4 as uuidV4 } from "../../../../../lib/uuid/index.ts";
import { OperatorsEnum } from "../../interfaces/index.ts";
import { makeProcessHelper } from "../tests/macro.ts";
import { GrandChatellerault2025 as Handler } from "./20250101_GrandChatellerault.ts";

// Châtellerault
const defaultPosition = {
  arr: "86066",
  com: "86066",
  aom: "248600413",
  epci: "248600413",
  dep: "86",
  reg: "75",
  country: "XXXXX",
};
const defaultLat = 46.81522088560449;
const defaultLon = 0.5471076683365941;

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
  datetime: new Date("2025-01-01"),
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
  "should work with exclusion",
  async () =>
    await process(
      {
        policy: { handler: Handler.id },
        carpool: [
          { distance: 4_999 },
          { distance: 80_000 },
          { operator_class: "A" },

          // // OD hors AOM
          {
            start: { ...defaultPosition, aom: "244900015" },
            end: { ...defaultPosition, aom: "244900015" },
          },

          // O dans une AOM exclue
          {
            start: { ...defaultPosition, aom: "200069854" },
            end: { ...defaultPosition, aom: "247200132" },
          },

          // D dans une AOM exclue
          {
            start: { ...defaultPosition, aom: "200071678" },
            end: { ...defaultPosition, aom: "200069854" },
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
          { distance: 1_000, driver_identity_key: "tom" },
          { distance: 5_000, driver_identity_key: "tom" },
          { distance: 5_000, seats: 2, driver_identity_key: "tom" },
          {
            distance: 5_000,
            driver_identity_key: "nina",
            passenger_identity_key: "marcel",
          },
          {
            distance: 80_000,
            driver_identity_key: "nina",
            passenger_identity_key: "marcel",
          },
        ],
        meta: [],
      },
      {
        incentive: [0, 150, 300, 150, 0],
        meta: [
          {
            key: "max_amount_restriction.global.campaign.global",
            value: 600n,
          },
          {
            key: "max_amount_restriction.0-tom.month.0-2025",
            value: 450n,
          },
          {
            key: "max_amount_restriction.0-nina.month.0-2025",
            value: 150n,
          },
        ],
      },
    ),
);

it(
  "should work with global limits",
  async () =>
    await process(
      {
        policy: { handler: Handler.id, max_amount: 2_200_000_00n },
        carpool: [{ distance: 5_000, driver_identity_key: "one" }],
        meta: [
          {
            key: "max_amount_restriction.global.campaign.global",
            value: 2_199_998_50n,
          },
        ],
      },
      {
        incentive: [150],
        meta: [
          {
            key: "max_amount_restriction.global.campaign.global",
            value: 2_200_000_00n,
          },
          {
            key: "max_amount_restriction.0-one.month.0-2025",
            value: 150n,
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
          {
            distance: 5_000,
            driver_identity_key: "one",
            passenger_identity_key: "two",
          },
          {
            distance: 5_000,
            driver_identity_key: "one",
            passenger_identity_key: "two",
          },
          {
            distance: 5_000,
            driver_identity_key: "one",
            passenger_identity_key: "three",
          },
          {
            distance: 5_000,
            driver_identity_key: "one",
            passenger_identity_key: "three",
          },
          {
            distance: 5_000,
            driver_identity_key: "one",
            passenger_identity_key: "four",
          },
          {
            distance: 5_000,
            driver_identity_key: "one",
            passenger_identity_key: "four",
          },
          {
            distance: 5_000,
            driver_identity_key: "one",
            passenger_identity_key: "five",
          },
        ],
        meta: [],
      },
      {
        incentive: [150, 150, 150, 150, 150, 150, 0],
        meta: [
          {
            key: "max_amount_restriction.global.campaign.global",
            value: 900n,
          },
          {
            key: "max_amount_restriction.0-one.month.0-2025",
            value: 900n,
          },
        ],
      },
    ),
);

it(
  "should work with driver month limits of 120,00 €",
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
            value: 0n,
          },
          {
            key: "max_amount_restriction.0-one.month.0-2025",
            value: 118_50n,
          },
        ],
      },
      {
        incentive: [150, 0],
        meta: [
          {
            key: "max_amount_restriction.global.campaign.global",
            value: 150n,
          },
          {
            key: "max_amount_restriction.0-one.month.0-2025",
            value: 120_00n,
          },
        ],
      },
    ),
);
