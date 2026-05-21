import { it } from "dep:testing-bdd";
import { v4 as uuidV4 } from "../../../../../lib/uuid/index.ts";
import { OperatorsEnum } from "../../interfaces/index.ts";
import { makeProcessHelper } from "../tests/macro.ts";
import { Vitre2023 as Handler } from "./20230101_Vitre.ts";

const defaultPosition = {
  arr: "35361",
  com: "35361",
  aom: "200039022",
  epci: "200039022",
  dep: "37",
  reg: "53",
  country: "XXXXX",
  reseau: 96,
};
const defaultLat = 48.72565703413325;
const defaultLon = 2.261827843187402;

const defaultCarpool = {
  _id: 1,
  operator_trip_id: uuidV4(),
  passenger_identity_key: uuidV4(),
  driver_identity_key: uuidV4(),
  operator_uuid: OperatorsEnum.KLAXIT,
  operator_class: "B",
  passenger_is_over_18: true,
  passenger_has_travel_pass: true,
  driver_has_travel_pass: true,
  datetime: new Date("2023-04-15"),
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
          { operator_uuid: "not in list" },
          { distance: 100 },
          { operator_class: "A" },
          { distance: 80_000, datetime: new Date("2023-07-17") },
          {
            // Vitré
            start: {
              com: "35360",
              aom: "200039022",
              epci: "200039022",
              reg: "53",
              arr: "35360",
            },
            // Occitanie (Ussel)
            end: {
              arr: "46240",
              com: "46240",
              aom: "200053791",
              epci: "200066371",
              reg: "76",
            },
            datetime: new Date("2023-07-17"),
          },
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
          { distance: 5_000, driver_identity_key: "one" },
          { distance: 5_000, seats: 2, driver_identity_key: "one" },
          {
            distance: 25_000,
            driver_identity_key: "one",
            passenger_identity_key: "two",
            start: {
              com: "35360",
              aom: "200039022",
              epci: "200039022",
              reg: "53",
              arr: "35360",
            },
            datetime: new Date("2023-07-01"),
          },
          {
            distance: 40_000,
            seats: 2,
            driver_identity_key: "one",
            passenger_identity_key: "three",
            datetime: new Date("2023-07-01"),
          },
          {
            distance: 25_000,
            driver_identity_key: "one",
            passenger_identity_key: "two",
            datetime: new Date("2023-07-20"),
          },
          {
            distance: 50_000,
            seats: 2,
            driver_identity_key: "one",
            passenger_identity_key: "three",
            datetime: new Date("2023-07-20"),
          },
        ],
        meta: [],
      },
      {
        incentive: [150, 300, 250, 600, 200, 400],
        meta: [
          {
            key: "max_amount_restriction.0-one.month.3-2023",
            value: 450,
          },
          {
            key: "max_amount_restriction.global.campaign.global",
            value: 1900,
          },
          {
            key: "max_amount_restriction.0-one.month.6-2023",
            value: 1450,
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
        policy: { handler: Handler.id, max_amount: 180_000_00 },
        carpool: [{ distance: 5_000, driver_identity_key: "one" }],
        meta: [
          {
            key: "max_amount_restriction.global.campaign.global",
            value: 179_999_50,
          },
        ],
      },
      {
        incentive: [50],
        meta: [
          {
            key: "max_amount_restriction.0-one.month.3-2023",
            value: 150,
          },
          {
            key: "max_amount_restriction.global.campaign.global",
            value: 180_000_00,
          },
        ],
      },
    ),
);

it(
  "should work with number day driver limit",
  async () =>
    await process(
      {
        policy: { handler: Handler.id },
        carpool: [
          {
            distance: 5_000,
            driver_identity_key: "one",
            passenger_identity_key: uuidV4(),
          },
          {
            distance: 5_000,
            driver_identity_key: "one",
            passenger_identity_key: uuidV4(),
          },
          {
            distance: 5_000,
            driver_identity_key: "one",
            passenger_identity_key: uuidV4(),
          },
          {
            distance: 5_000,
            driver_identity_key: "one",
            passenger_identity_key: uuidV4(),
          },
          {
            distance: 5_000,
            driver_identity_key: "one",
            passenger_identity_key: uuidV4(),
          },
          {
            distance: 5_000,
            driver_identity_key: "one",
            passenger_identity_key: uuidV4(),
          },
          {
            distance: 5_000,
            driver_identity_key: "one",
            passenger_identity_key: uuidV4(),
          },
        ],
        meta: [],
      },
      {
        incentive: [150, 150, 150, 150, 150, 150, 0],
        meta: [
          {
            key: "max_amount_restriction.0-one.month.3-2023",
            value: 900,
          },
          {
            key: "max_amount_restriction.global.campaign.global",
            value: 9_00,
          },
        ],
      },
    ),
);

it(
  "should work with number day passenger limit",
  async () =>
    await process(
      {
        policy: { handler: Handler.id },
        carpool: [
          { distance: 5_000, driver_identity_key: "one" },
          { distance: 5_000, driver_identity_key: "one" },
          { distance: 5_000, driver_identity_key: "one" },
        ],
        meta: [],
      },
      {
        incentive: [150, 150, 0],
        meta: [
          {
            key: "max_amount_restriction.0-one.month.3-2023",
            value: 300,
          },
          {
            key: "max_amount_restriction.global.campaign.global",
            value: 3_00,
          },
        ],
      },
    ),
);
