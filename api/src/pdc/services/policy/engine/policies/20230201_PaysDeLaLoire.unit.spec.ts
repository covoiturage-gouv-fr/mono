import { it } from "../../../../../dev_deps.ts";
import { v4 as uuidV4 } from "../../../../../lib/uuid/index.ts";
import { OperatorsEnum } from "../../interfaces/index.ts";
import { makeProcessHelper } from "../tests/macro.ts";
import { PaysDeLaLoire2023 as Handler } from "./20230201_PaysDeLaLoire.ts";

const defaultPosition = {
  arr: "85047",
  com: "85047",
  aom: "200071629",
  epci: "200071629",
  dep: "85",
  reg: "52",
  country: "XXXXX",
  reseau: 430,
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
          { distance: 100 },
          { operator_class: "A" },
          {
            start: { ...defaultPosition, aom: "244900015" },
            end: { ...defaultPosition, aom: "244900015" },
          },
          {
            start: { ...defaultPosition, aom: "244400404" },
            end: { ...defaultPosition, aom: "244400404" },
          },
          {
            start: { ...defaultPosition, aom: "247200132" },
            end: { ...defaultPosition, aom: "247200132" },
          },
          {
            start: { ...defaultPosition, aom: "200071678" },
            end: { ...defaultPosition, aom: "200071678" },
          },
          { start: { ...defaultPosition, reg: "11" } },
          { end: { ...defaultPosition, reg: "11" } },
          { distance: 90_000 },
          { passenger_is_over_18: false },
        ],
        meta: [],
      },
      { incentive: [0, 0, 0, 0, 0, 0, 0, 0, 0, 0], meta: [] },
    ),
);

it(
  "should work basic",
  async () =>
    await process(
      {
        policy: { handler: Handler.id },
        carpool: [
          { distance: 1_000, driver_identity_key: "one" },
          { distance: 5_000, driver_identity_key: "one" },
          { distance: 5_000, seats: 2, driver_identity_key: "one" },
          { distance: 20_000, driver_identity_key: "two" },
          { distance: 25_000, driver_identity_key: "two" },
          {
            distance: 25_000,
            driver_identity_key: "two",
            datetime: new Date("2023-05-28"),
          },
          { distance: 40_000, driver_identity_key: "two" },
          { distance: 55_000, driver_identity_key: "two" },
          { distance: 80_000, driver_identity_key: "two" },
          { distance: 90_000, driver_identity_key: "two" },
        ],
        meta: [],
      },
      {
        incentive: [0, 100, 200, 100, 150, 150, 300, 300, 300, 0],
        meta: [
          {
            key: "max_amount_restriction.global.campaign.global",
            value: 1600n,
          },
          {
            key: "max_amount_restriction.0-one.month.3-2023",
            value: 300n,
          },
          {
            key: "max_amount_restriction.0-two.month.3-2023",
            value: 1150n,
          },
          {
            key: "max_amount_restriction.0-two.month.4-2023",
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
        policy: { handler: Handler.id, max_amount: 500_000_00n },
        carpool: [{ distance: 5_000, driver_identity_key: "one" }],
        meta: [
          {
            key: "max_amount_restriction.global.campaign.global",
            value: 499_999_50n,
          },
        ],
      },
      {
        incentive: [50],
        meta: [
          {
            key: "max_amount_restriction.global.campaign.global",
            value: 500_000_00n,
          },
          {
            key: "max_amount_restriction.0-one.month.3-2023",
            value: 100n,
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
        incentive: [100, 100, 100, 100, 100, 100, 0],
        meta: [
          {
            key: "max_amount_restriction.global.campaign.global",
            value: 600n,
          },
          {
            key: "max_amount_restriction.0-one.month.3-2023",
            value: 600n,
          },
        ],
      },
    ),
);
