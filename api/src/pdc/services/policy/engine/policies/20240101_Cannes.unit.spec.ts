import { it } from "../../../../../dev_deps.ts";
import { v4 as uuidV4 } from "../../../../../lib/uuid/index.ts";
import { OperatorsEnum } from "../../interfaces/index.ts";
import { makeProcessHelper } from "../tests/macro.ts";
import { Cannes2024 as Handler } from "./20240101_Cannes.ts";

const defaultPosition = {
  arr: "74278",
  com: "74278",
  aom: "200033116",
  epci: "200033116",
  dep: "74",
  reg: "84",
  country: "XXXXX",
  reseau: 142,
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
  datetime: new Date("2024-05-15"),
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
          distance: 80_001,
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
          { distance: 30_000, driver_identity_key: "one" },
          { distance: 60_000, driver_identity_key: "one" },
        ],
        meta: [],
      },
      {
        incentive: [150, 300, 300, 300],
        meta: [
          {
            key: "max_amount_restriction.0-one.month.4-2024",
            value: 1050n,
          },
          {
            key: "max_amount_restriction.global.campaign.global",
            value: 1050n,
          },
        ],
      },
    ),
);

it(
  "should work with driver month limits 150",
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
            key: "max_amount_restriction.0-one.month.4-2024",
            value: 148_50n,
          },
        ],
      },
      {
        incentive: [150, 0],
        meta: [
          {
            key: "max_amount_restriction.0-one.month.4-2024",
            value: 150_00n,
          },
          {
            key: "max_amount_restriction.global.campaign.global",
            value: 150n,
          },
        ],
      },
    ),
);
