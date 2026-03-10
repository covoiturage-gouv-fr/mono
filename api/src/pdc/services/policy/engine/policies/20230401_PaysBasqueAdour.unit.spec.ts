import { it } from "dep:testing-bdd";
import { v4 as uuidV4 } from "../../../../../lib/uuid/index.ts";
import { OperatorsEnum } from "../../interfaces/index.ts";
import { makeProcessHelper } from "../tests/macro.ts";
import { PaysBasque20232024 as Handler } from "./20230401_PaysBasqueAdour.ts";

const defaultPosition = {
  arr: "64155",
  com: "64155",
  aom: "256401605",
  epci: "200067106",
  dep: "64",
  reg: "75",
  country: "XXXXX",
  reseau: 15,
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
  datetime: new Date("2023-04-15"),
  seats: 1,
  distance: 6_000,
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
          { operator_class: "A" },
          {
            start: {
              arr: "37109",
              com: "37109",
              aom: "200085108",
              epci: "243700754",
              reg: "24",
            },
            end: {
              arr: "37109",
              com: "37109",
              aom: "200085108",
              epci: "243700754",
              reg: "24",
            },
          },
          { passenger_is_over_18: false },
          { distance: 90_000, datetime: new Date("2024-01-01") },
          {
            datetime: new Date("2024-01-01"),
            operator_uuid: OperatorsEnum.MOBICOOP,
          },
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
          { distance: 5_000, driver_identity_key: "one" },
          { distance: 5_000, seats: 2, driver_identity_key: "one" },
          { distance: 25_000, driver_identity_key: "one" },
          { distance: 25_000, seats: 2, driver_identity_key: "one" },
          {
            distance: 35_000,
            driver_identity_key: "one",
          },
          {
            distance: 90_000,
            datetime: new Date("2023-12-31"),
            driver_identity_key: "one",
          },
          { distance: 80_000, driver_identity_key: "one" },
        ],
        meta: [],
      },
      {
        incentive: [200, 400, 250, 500, 300, 300, 300],
        meta: [
          {
            key: "max_amount_restriction.0-one.month.3-2023",
            value: 1950n,
          },
          {
            key: "max_amount_restriction.global.campaign.global",
            value: 2250n,
          },
          {
            key: "max_amount_restriction.0-one.month.11-2023",
            value: 300n,
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
        policy: { handler: Handler.id, max_amount: 60_000_00n },
        carpool: [{ distance: 5_000, driver_identity_key: "one" }],
        meta: [
          {
            key: "max_amount_restriction.global.campaign.global",
            value: 49_998_00n,
          },
        ],
      },
      {
        incentive: [200],
        meta: [
          {
            key: "max_amount_restriction.0-one.month.3-2023",
            value: 200n,
          },
          {
            key: "max_amount_restriction.global.campaign.global",
            value: 50_000_00n,
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
        incentive: [200, 200, 200, 200, 200, 200, 0],
        meta: [
          {
            key: "max_amount_restriction.0-one.month.3-2023",
            value: 1200n,
          },
          {
            key: "max_amount_restriction.global.campaign.global",
            value: 12_00n,
          },
        ],
      },
    ),
);
