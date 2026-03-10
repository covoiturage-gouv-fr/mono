import { it } from "dep:testing-bdd";
import { v4 as uuidV4 } from "../../../../../lib/uuid/index.ts";
import { OperatorsEnum } from "../../interfaces/index.ts";
import { makeProcessHelper } from "../tests/macro.ts";
import { PetrLunevilloisS12023 as Handler } from "./20240108_PetrLunevillois.ts";

const defaultPosition = {
  arr: "54233",
  com: "54233",
  aom: "200051134",
  epci: "200069433",
  dep: "54",
  reg: "44",
  country: "XXXXX",
  reseau: 269,
};
const defaultLat = 48.5905360901711;
const defaultLon = 6.499392987670189;

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
  datetime: new Date("2024-01-08"),
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
  end_lat: 48.58685290576798,
  end_lon: 6.483696700766759,
};

const process = makeProcessHelper(defaultCarpool);

it(
  "should work with exclusions",
  async () =>
    await process(
      {
        policy: { handler: Handler.id },
        carpool: [
          { operator_uuid: "not in list" },
          { operator_uuid: OperatorsEnum.KLAXIT },
          { distance: 100 },
          { distance: 60_000 },
          { operator_class: "A" },
        ],
        meta: [],
      },
      { incentive: [0, 0, 0, 0, 0], meta: [] },
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
        ],
        meta: [],
      },
      {
        incentive: [21, 42, 161, 322],
        meta: [
          {
            key: "max_amount_restriction.global.campaign.global",
            value: 546n,
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
          // start
          {
            distance: 5_000,
            driver_identity_key: "one",
            start: { ...defaultPosition, aom: "not_in_aom" },
          },
          {
            distance: 5_000,
            seats: 2,
            driver_identity_key: "one",
            start: { ...defaultPosition, aom: "not_in_aom" },
          },
          {
            distance: 25_000,
            driver_identity_key: "two",
            start: { ...defaultPosition, aom: "not_in_aom" },
          },
          {
            distance: 25_000,
            seats: 2,
            driver_identity_key: "two",
            start: { ...defaultPosition, aom: "not_in_aom" },
          },
          {
            distance: 55_000,
            driver_identity_key: "two",
            start: { ...defaultPosition, aom: "not_in_aom" },
          },

          // end
          {
            distance: 5_000,
            driver_identity_key: "one",
            end: { ...defaultPosition, aom: "not_in_aom" },
          },
          {
            distance: 5_000,
            seats: 2,
            driver_identity_key: "one",
            end: { ...defaultPosition, aom: "not_in_aom" },
          },
          {
            distance: 25_000,
            driver_identity_key: "two",
            end: { ...defaultPosition, aom: "not_in_aom" },
          },
          {
            distance: 25_000,
            seats: 2,
            driver_identity_key: "two",
            end: { ...defaultPosition, aom: "not_in_aom" },
          },
          {
            distance: 55_000,
            driver_identity_key: "two",
            end: { ...defaultPosition, aom: "not_in_aom" },
          },
        ],
        meta: [],
      },
      {
        incentive: [0, 0, 0, 0, 0, 0, 0, 0, 0, 0],
        meta: [],
      },
    ),
);

it(
  "should work with global limits",
  async () =>
    await process(
      {
        policy: { handler: Handler.id, max_amount: 10_000_00 },
        carpool: [{ distance: 59_000, driver_identity_key: "one" }],
        meta: [
          {
            key: "max_amount_restriction.global.campaign.global",
            value: 9_999_99n,
          },
        ],
      },
      {
        incentive: [1], // <-- should be 413. capped to 1
        meta: [
          {
            key: "max_amount_restriction.global.campaign.global",
            value: 10_000_00n,
          },
        ],
      },
    ),
);

it(
  "should work with 2 trips per day limit",
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
        meta: [],
      },
      {
        incentive: [21, 21, 0, 0],
        meta: [
          {
            key: "max_amount_restriction.global.campaign.global",
            value: 42n,
          },
        ],
      },
    ),
);
