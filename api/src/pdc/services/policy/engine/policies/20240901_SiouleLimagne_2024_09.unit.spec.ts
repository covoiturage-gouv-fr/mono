import { it } from "../../../../../dev_deps.ts";
import { v4 as uuidV4 } from "../../../../../lib/uuid/index.ts";
import { OperatorsEnum } from "../../interfaces/index.ts";
import { makeProcessHelper } from "../tests/macro.ts";
import { SiouleLimagne as Handler } from "./20240901_SiouleLimagne_2024_09.ts";

// Toul
const defaultPosition = {
  arr: "54528",
  com: "54528",
  aom: "200070563",
  epci: "200066645",
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
  operator_uuid: OperatorsEnum.BLABLACAR_DAILY,
  operator_class: "C",
  passenger_is_over_18: true,
  passenger_has_travel_pass: true,
  driver_has_travel_pass: true,
  datetime: new Date("2024-09-16"),
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
          { distance: 1_999 },
          { distance: 80_001 },
          { operator_class: "A" },
          { operator_uuid: OperatorsEnum.KAROS },
          { passenger_is_over_18: false },
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
            distance: 1_000,
            driver_identity_key: uuidV4(),
            passenger_identity_key: uuidV4(),
          },
          {
            distance: 5_000,
            driver_identity_key: uuidV4(),
            passenger_identity_key: uuidV4(),
            datetime: new Date("2024-09-16"),
          },
          {
            distance: 5_000,
            driver_identity_key: uuidV4(),
            passenger_identity_key: uuidV4(),
            datetime: new Date("2024-10-14"),
            seats: 2,
          },
          {
            distance: 20_000,
            driver_identity_key: uuidV4(),
            passenger_identity_key: uuidV4(),
            datetime: new Date("2024-10-15"),
          },
          {
            distance: 40_000,
            driver_identity_key: uuidV4(),
            passenger_identity_key: uuidV4(),
            datetime: new Date("2025-08-30"),
            seats: 2,
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
        incentive: [0, 200, 400, 150, 300, 0],
      },
    ),
);

it(
  "should work with global limits",
  async () =>
    await process(
      {
        policy: { handler: Handler.id, max_amount: 3_000_00n },
        carpool: [{ distance: 5_000, driver_identity_key: "one" }],
        meta: [
          {
            key: "max_amount_restriction.global.campaign.global",
            value: 2_998_00n,
          },
        ],
      },
      {
        incentive: [200],
        meta: [
          {
            key: "max_amount_restriction.global.campaign.global",
            value: 3_000_00n,
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
          { distance: 5_000, driver_identity_key: "driver_A" },
          { distance: 5_000, driver_identity_key: "driver_A" },
          { distance: 5_000, driver_identity_key: "driver_A" },
          { distance: 5_000, driver_identity_key: "driver_A" },
        ],
        meta: [],
      },
      {
        incentive: [200, 200, 200, 200, 200, 200, 0],
        meta: [
          {
            key: "max_amount_restriction.global.campaign.global",
            value: 1200n,
          },
        ],
      },
    ),
);
