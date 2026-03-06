import { it } from "dep:testing-bdd";
import { v4 as uuidV4 } from "../../../../../lib/uuid/index.ts";
import { OperatorsEnum } from "../../interfaces/index.ts";
import { makeProcessHelper } from "../tests/macro.ts";
import { SMT2022 as Handler } from "./20220414_SMT.ts";

const defaultPosition = {
  arr: "37261",
  com: "37261",
  aom: "200085108",
  epci: "243700754",
  dep: "37",
  reg: "24",
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
  operator_class: "C",
  passenger_is_over_18: true,
  passenger_has_travel_pass: true,
  driver_has_travel_pass: true,
  datetime: new Date("2022-04-15"),
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
        carpool: [{ operator_uuid: "not in list" }, { distance: 100 }, {
          distance: 200_000,
        }, { operator_class: "A" }],
        meta: [],
      },
      { incentive: [0, 0, 0, 0], meta: [] },
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
          { distance: 25_000, driver_identity_key: "two" },
          {
            distance: 25_000,
            driver_identity_key: "two",
            datetime: new Date("2022-03-28"),
          },
          { distance: 55_000, driver_identity_key: "two" },
        ],
        meta: [],
      },
      {
        incentive: [200, 400, 250, 250, 400],
        meta: [
          {
            key: "max_amount_restriction.global.campaign.global",
            value: 1500n,
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
        policy: { handler: Handler.id, max_amount: 40_000_00n },
        carpool: [{ distance: 5_000, driver_identity_key: "one" }],
        meta: [
          {
            key: "max_amount_restriction.global.campaign.global",
            value: 39_999_50n,
          },
        ],
      },
      {
        incentive: [50],
        meta: [
          {
            key: "max_amount_restriction.global.campaign.global",
            value: 40_000_00n,
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
            key: "max_amount_restriction.global.campaign.global",
            value: 12_00n,
          },
        ],
      },
    ),
);
