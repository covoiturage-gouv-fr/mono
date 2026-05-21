import { it } from "dep:testing-bdd";
import { v4 as uuidV4 } from "../../../../../lib/uuid/index.ts";
import { OperatorsEnum } from "../../interfaces/index.ts";
import { makeProcessHelper } from "../tests/macro.ts";
import { PMGF2022 as Handler } from "./20221102_PMGF.ts";

const defaultPosition = {
  arr: "74008",
  com: "74008",
  aom: "200011773",
  epci: "200011773",
  dep: "74",
  reg: "84",
  country: "XXXXX",
  reseau: 10,
};
const defaultLat = 48.72565703413325;
const defaultLon = 2.261827843187402;

const defaultCarpool = {
  _id: 1,
  operator_trip_id: uuidV4(),
  passenger_identity_key: uuidV4(),
  driver_identity_key: uuidV4(),
  operator_uuid: OperatorsEnum.BLABLACAR_DAILY,
  operator_class: "C",
  passenger_is_over_18: true,
  passenger_has_travel_pass: true,
  driver_has_travel_pass: true,
  datetime: new Date("2022-11-15"),
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
          { operator_uuid: OperatorsEnum.MOBICOOP },
        ],
        meta: [],
      },
      { incentive: [0, 0, 0, 0], meta: [] },
    ),
);

it(
  "should work basic with start/end inside aom",
  async () =>
    await process(
      {
        policy: { handler: Handler.id },
        carpool: [
          { distance: 5_000, driver_identity_key: "one" },
          { distance: 5_000, seats: 2, driver_identity_key: "one" },
          { distance: 25_000, driver_identity_key: "two" },
          { distance: 25_000, seats: 2, driver_identity_key: "two" },
          { distance: 55_000, driver_identity_key: "two" },
        ],
        meta: [],
      },
      {
        incentive: [200, 400, 250, 500, 400],
        meta: [
          {
            key: "max_amount_restriction.0-one.month.10-2022",
            value: 600,
          },
          {
            key: "max_amount_restriction.global.campaign.global",
            value: 1750,
          },
          {
            key: "max_amount_restriction.0-two.month.10-2022",
            value: 1150,
          },
        ],
      },
    ),
);

it(
  "should work basic with start or end outside aom",
  async () =>
    await process(
      {
        policy: { handler: Handler.id },
        carpool: [
          {
            distance: 5_000,
            driver_identity_key: "one",
            start: { ...defaultPosition, arr: "not_in_aom" },
          },
          {
            distance: 5_000,
            seats: 2,
            driver_identity_key: "one",
            start: { ...defaultPosition, arr: "not_in_aom" },
          },
          {
            distance: 25_000,
            driver_identity_key: "two",
            start: { ...defaultPosition, arr: "not_in_aom" },
          },
          {
            distance: 25_000,
            seats: 2,
            driver_identity_key: "two",
            start: { ...defaultPosition, arr: "not_in_aom" },
          },
          {
            distance: 55_000,
            driver_identity_key: "two",
            start: { ...defaultPosition, arr: "not_in_aom" },
          },
        ],
        meta: [],
      },
      {
        incentive: [100, 200, 150, 300, 300],
        meta: [
          {
            key: "max_amount_restriction.0-one.month.10-2022",
            value: 300,
          },
          {
            key: "max_amount_restriction.global.campaign.global",
            value: 1050,
          },
          {
            key: "max_amount_restriction.0-two.month.10-2022",
            value: 750,
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
        policy: { handler: Handler.id, max_amount: 100_000_00 },
        carpool: [{ distance: 5_000, driver_identity_key: "one" }],
        meta: [
          {
            key: "max_amount_restriction.global.campaign.global",
            value: 99_999_50,
          },
        ],
      },
      {
        incentive: [50],
        meta: [
          {
            key: "max_amount_restriction.0-one.month.10-2022",
            value: 200,
          },
          {
            key: "max_amount_restriction.global.campaign.global",
            value: 100_000_00,
          },
        ],
      },
    ),
);

it(
  "should include Mobicoop since 02 january 2023",
  async () =>
    await process(
      {
        policy: { handler: Handler.id },
        carpool: [
          {
            distance: 5_000,
            driver_identity_key: "one",
            operator_uuid: OperatorsEnum.MOBICOOP,
            datetime: new Date("2023-01-02"),
          },
        ],
        meta: [],
      },
      {
        incentive: [200],
        meta: [
          {
            key: "max_amount_restriction.0-one.month.0-2023",
            value: 200,
          },
          {
            key: "max_amount_restriction.global.campaign.global",
            value: 200,
          },
        ],
      },
    ),
);

it(
  "should work with month limits",
  async () =>
    await process(
      {
        policy: { handler: Handler.id },
        carpool: [
          { distance: 5_000, driver_identity_key: "one" },
          { distance: 5_000, driver_identity_key: "one" },
          { distance: 5_000, driver_identity_key: "one" },
          { distance: 5_000, driver_identity_key: "one" },
        ],
        meta: [
          {
            key: "max_amount_restriction.0-one.month.10-2022",
            value: 11500,
          },
        ],
      },
      {
        incentive: [200, 200, 100, 0],
        meta: [
          {
            key: "max_amount_restriction.0-one.month.10-2022",
            value: 12000,
          },
          {
            key: "max_amount_restriction.global.campaign.global",
            value: 500,
          },
        ],
      },
    ),
);
