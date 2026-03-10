import { it } from "dep:testing-bdd";
import { v4 as uuidV4 } from "../../../../../lib/uuid/index.ts";
import { OperatorsEnum } from "../../interfaces/index.ts";
import { makeProcessHelper } from "../tests/macro.ts";
import { MetropoleSavoie as Handler } from "./20230124_MetropoleSavoie.ts";

const defaultPosition = {
  arr: "73031",
  com: "73031",
  aom: "200069110",
  epci: "200069110",
  dep: "73",
  reg: "84",
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
  operator_uuid: OperatorsEnum.BLABLACAR_DAILY,
  operator_class: "C",
  passenger_is_over_18: true,
  passenger_has_travel_pass: true,
  driver_has_travel_pass: true,
  datetime: new Date("2023-01-25"),
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
  "should work basic",
  async () =>
    await process(
      {
        policy: { handler: Handler.id },
        carpool: [
          { distance: 6_000 },
          { distance: 6_000, seats: 2 },
          { distance: 80_000 },
          { distance: 80_000, seats: 2 },
          {
            distance: 5_000,
            start: {
              aom: "",
              com: "73084",
              arr: "73084",
              epci: "200041010",
              reg: "84",
            },
          },
        ],
        meta: [],
      },
      {
        incentive: [200, 400, 800, 1600, 200],
        meta: [
          {
            key: "max_amount_restriction.global.campaign.global",
            value: 3200n,
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
        policy: { handler: Handler.id, max_amount: 150_000_00n },
        carpool: [{ distance: 5_000, driver_identity_key: "one" }],
        meta: [
          {
            key: "max_amount_restriction.global.campaign.global",
            value: 149_999_50n,
          },
        ],
      },
      {
        incentive: [50],
        meta: [
          {
            key: "max_amount_restriction.global.campaign.global",
            value: 150_000_00n,
          },
        ],
      },
    ),
);

it(
  "should work with exclusion",
  async () =>
    await process(
      {
        policy: { handler: Handler.id },
        carpool: [
          {
            distance: 25_000,
            operator_uuid: OperatorsEnum.KLAXIT,
          },
          {
            distance: 25_000,
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
        incentive: [0, 0],
        meta: [],
      },
    ),
);
