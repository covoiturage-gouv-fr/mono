import { it } from "../../../../../dev_deps.ts";
import { v4 as uuidV4 } from "../../../../../lib/uuid/index.ts";
import { OperatorsEnum } from "../../interfaces/index.ts";
import { makeProcessHelper } from "../tests/macro.ts";
import { NantesMetropoleXPCovoitan2021 as Handler } from "./20211202_NantesMetropoleXP.ts";

const defaultPosition = {
  arr: "44026",
  com: "44026",
  aom: "244400404",
  epci: "244400404",
  dep: "44",
  reg: "52",
  country: "XXXXX",
  reseau: 67,
};
const defaultLat = 48.72565703413325;
const defaultLon = 2.261827843187402;

const defaultCarpool = {
  _id: 1,
  operator_trip_id: uuidV4(),
  passenger_identity_key: uuidV4(),
  driver_identity_key: uuidV4(),
  operator_uuid: OperatorsEnum.KLAXIT,
  operator_class: "C",
  passenger_is_over_18: true,
  passenger_has_travel_pass: true,
  driver_has_travel_pass: true,
  datetime: new Date("2021-12-03"),
  seats: 1,
  distance: 19_000,
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
          { operator_uuid: "not in list" },
          { distance: 100 },
          { start: { ...defaultPosition, aom: "not_ok" } },
          { end: { ...defaultPosition, aom: "not_ok" } },
          { operator_class: "A" },
        ],
        meta: [],
      },
      { incentive: [0, 0, 0, 0, 0], meta: [] },
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
            distance: 5_000,
            driver_identity_key: "one",
            operator_trip_id: "1",
          },
          {
            distance: 5_000,
            seats: 2,
            driver_identity_key: "one",
            operator_trip_id: "2",
          },
          {
            distance: 25_000,
            driver_identity_key: "two",
            seats: 2,
            operator_trip_id: "3",
          },
          {
            distance: 25_000,
            driver_identity_key: "two",
            datetime: new Date("2022-03-28"),
          },
        ],
        meta: [],
      },
      {
        incentive: [200, 400, 500, 250],
        meta: [
          {
            key: "max_trip_restriction.global.campaign.global",
            value: 4n,
          },
          {
            key: "max_amount_restriction.global.campaign.global",
            value: 1350n,
          },
        ],
      },
    ),
);

it(
  "should work with global amount limit",
  async () =>
    await process(
      {
        policy: { handler: Handler.id, max_amount: 10_000_000_00n },
        carpool: [{ distance: 5_000, driver_identity_key: "one" }],
        meta: [
          {
            key: "max_trip_restriction.global.campaign.global",
            value: 2n,
          },
          {
            key: "max_amount_restriction.global.campaign.global",
            value: 9_999_999_50n,
          },
        ],
      },
      {
        incentive: [50],
        meta: [
          {
            key: "max_trip_restriction.global.campaign.global",
            value: 3n,
          },
          {
            key: "max_amount_restriction.global.campaign.global",
            value: 10_000_000_00n,
          },
        ],
      },
    ),
);

it(
  "should work with global trip limit",
  async () =>
    await process(
      {
        policy: { handler: Handler.id },
        carpool: [
          { distance: 5_000, driver_identity_key: "one" },
          { distance: 5_000, driver_identity_key: "one" },
        ],
        meta: [
          {
            key: "max_trip_restriction.global.campaign.global",
            value: 9_999_999n,
          },
          {
            key: "max_amount_restriction.global.campaign.global",
            value: 50n,
          },
        ],
      },
      {
        incentive: [200, 0],
        meta: [
          {
            key: "max_trip_restriction.global.campaign.global",
            value: 10_000_000n,
          },
          {
            key: "max_amount_restriction.global.campaign.global",
            value: 250n,
          },
        ],
      },
    ),
);

it(
  "should work with driver day limits",
  async () =>
    await process(
      {
        policy: { handler: Handler.id },
        carpool: [
          {
            distance: 5_000,
            driver_identity_key: "11",
            passenger_identity_key: "21",
            operator_trip_id: "1",
          },
          {
            distance: 5_000,
            driver_identity_key: "11",
            passenger_identity_key: "22",
            operator_trip_id: "2",
          },
          {
            distance: 5_000,
            driver_identity_key: "11",
            passenger_identity_key: "23",
            operator_trip_id: "3",
          },
          {
            distance: 5_000,
            driver_identity_key: "11",
            passenger_identity_key: "24",
            operator_trip_id: "4",
          },
          {
            distance: 5_000,
            driver_identity_key: "11",
            passenger_identity_key: "25",
            operator_trip_id: "5",
          },
          {
            distance: 5_000,
            driver_identity_key: "11",
            passenger_identity_key: "26",
            operator_trip_id: "6",
          },
        ],
        meta: [],
      },
      {
        incentive: [200, 200, 200, 200, 0, 0],
        meta: [
          {
            key: "max_trip_restriction.global.campaign.global",
            value: 4n,
          },
          {
            key: "max_amount_restriction.global.campaign.global",
            value: 800n,
          },
        ],
      },
    ),
);

it(
  "should work with passenger day limits",
  async () =>
    await process(
      {
        policy: { handler: Handler.id },
        carpool: [
          {
            distance: 5_000,
            passenger_identity_key: "11",
            driver_identity_key: "21",
            operator_trip_id: "1",
          },
          {
            distance: 5_000,
            passenger_identity_key: "11",
            driver_identity_key: "22",
            operator_trip_id: "2",
          },
          {
            distance: 5_000,
            passenger_identity_key: "11",
            driver_identity_key: "23",
            operator_trip_id: "3",
          },
          {
            distance: 5_000,
            passenger_identity_key: "11",
            driver_identity_key: "24",
            operator_trip_id: "4",
          },
          {
            distance: 5_000,
            passenger_identity_key: "11",
            driver_identity_key: "25",
            operator_trip_id: "5",
          },
          {
            distance: 5_000,
            passenger_identity_key: "11",
            driver_identity_key: "26",
            operator_trip_id: "6",
          },
        ],
        meta: [],
      },
      {
        incentive: [200, 200, 200, 200, 0, 0],
        meta: [
          {
            key: "max_trip_restriction.global.campaign.global",
            value: 4n,
          },
          {
            key: "max_amount_restriction.global.campaign.global",
            value: 800n,
          },
        ],
      },
    ),
);

it(
  "should work with passenger trip limit",
  async () =>
    await process(
      {
        policy: { handler: Handler.id },
        carpool: [
          {
            distance: 5_000,
            passenger_identity_key: "11",
            driver_identity_key: "21",
            operator_trip_id: "1",
          },
          {
            distance: 5_000,
            passenger_identity_key: "12",
            driver_identity_key: "22",
            operator_trip_id: "1",
            seats: 2,
          },
          {
            distance: 5_000,
            passenger_identity_key: "13",
            driver_identity_key: "23",
            operator_trip_id: "1",
          },
          {
            distance: 5_000,
            passenger_identity_key: "14",
            driver_identity_key: "24",
            operator_trip_id: "1",
          },
        ],
        meta: [],
      },
      {
        incentive: [200, 400, 0, 0],
        meta: [
          {
            key: "max_trip_restriction.global.campaign.global",
            value: 2n,
          },
          {
            key: "max_amount_restriction.global.campaign.global",
            value: 600n,
          },
        ],
      },
    ),
);
