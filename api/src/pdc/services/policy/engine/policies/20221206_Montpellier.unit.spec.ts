import { it } from "../../../../../dev_deps.ts";
import { v4 as uuidV4 } from "../../../../../lib/uuid/index.ts";
import { OperatorsEnum } from "../../interfaces/index.ts";
import { generatePartialCarpools } from "../tests/helpers.ts";
import { makeProcessHelper } from "../tests/macro.ts";
import { Montpellier as Handler } from "./20221206_Montpellier.ts";

const defaultPosition = {
  arr: "34088",
  com: "34088",
  aom: "243400017",
  epci: "243400017",
  dep: "34",
  reg: "76",
  country: "XXXXX",
  reseau: 76,
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
  datetime: new Date("2022-12-07"),
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
            distance: 25_000,
            driver_identity_key: "one",
            start: {
              aom: "200096956",
              com: "47091",
              arr: "47091",
              epci: "200096956",
              reg: "75",
            },
          },
        ],
        meta: [],
      },
      {
        incentive: [100, 200, 200, 400, 200],
        meta: [
          {
            key: "max_amount_restriction.0-one.month.11-2022",
            value: 1100n,
          },
          {
            key: "max_amount_restriction.global.campaign.global",
            value: 1100n,
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
          { distance: 25_000, driver_identity_key: "two" },
          { distance: 25_000, driver_identity_key: "two" },
          { distance: 25_000, driver_identity_key: "two" },
          { distance: 25_000, driver_identity_key: "two" },
          { distance: 25_000, driver_identity_key: "two" },
          { distance: 25_000, driver_identity_key: "two" },
          { distance: 25_000, driver_identity_key: "two" },
        ],
        meta: [],
      },
      {
        incentive: [200, 200, 200, 200, 200, 200, 0],
        meta: [
          {
            key: "max_amount_restriction.0-two.month.11-2022",
            value: 12_00n,
          },
          {
            key: "max_amount_restriction.global.campaign.global",
            value: 12_00n,
          },
        ],
      },
    ),
);

it(
  "should work with driver month limits",
  async () =>
    await process(
      {
        policy: { handler: Handler.id },
        carpool: generatePartialCarpools(),
        meta: [],
      },
      {
        incentive: [...[...Array(75).keys()].map(() => 200), 0],
        meta: [
          {
            key: "max_amount_restriction.0-three.month.0-2022",
            value: 150_00n,
          },
          {
            key: "max_amount_restriction.global.campaign.global",
            value: 15000n,
          },
        ],
      },
    ),
);
