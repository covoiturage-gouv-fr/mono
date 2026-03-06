import { assertEquals, assertNotEquals } from "dep:assert";
import { it } from "dep:testing-bdd";
import { v4 as uuidV4 } from "../../../../../lib/uuid/index.ts";
import { OperatorsEnum } from "../../interfaces/index.ts";
import { makeProcessHelper } from "../tests/macro.ts";
import { Cotentin2023 as Handler } from "./20230101_Cotentin.ts";

const defaultPosition = {
  arr: "22113",
  com: "22113",
  aom: "200065928",
  epci: "200065928",
  dep: "22",
  reg: "53",
  country: "XXXXX",
  reseau: 194,
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
  datetime: new Date("2023-01-15"),
  seats: 1,
  distance: 19_000,
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
          operator_class: "A",
        }],
        meta: [],
      },
      { incentive: [0, 0, 0], meta: [] },
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
            distance: 5_000,
            driver_identity_key: "one",
            operator_trip_id: "1",
          },
          {
            distance: 5_000,
            seats: 2,
            driver_identity_key: "one",
            operator_trip_id: "2",
          },
          {
            distance: 25_000,
            driver_identity_key: "two",
            seats: 2,
            operator_trip_id: "3",
          },
          {
            distance: 25_000,
            driver_identity_key: "two",
            datetime: new Date("2023-03-28"),
          },
        ],
        meta: [],
      },
      {
        incentive: [200, 400, 500, 250],
        meta: [
          {
            key: "max_amount_restriction.global.campaign.global",
            value: 1350n,
          },
        ],
      },
    ),
);

it(
  "should work with driver day limits",
  async () =>
    await process(
      {
        policy: { handler: Handler.id },
        carpool: [
          {
            distance: 5_000,
            driver_identity_key: "11",
            passenger_identity_key: "21",
            operator_trip_id: "1",
          },
          {
            distance: 5_000,
            driver_identity_key: "11",
            passenger_identity_key: "22",
            operator_trip_id: "2",
          },
          {
            distance: 5_000,
            driver_identity_key: "11",
            passenger_identity_key: "23",
            operator_trip_id: "3",
          },
          {
            distance: 5_000,
            driver_identity_key: "11",
            passenger_identity_key: "24",
            operator_trip_id: "4",
          },
          {
            distance: 5_000,
            driver_identity_key: "11",
            passenger_identity_key: "25",
            operator_trip_id: "5",
          },
          {
            distance: 5_000,
            driver_identity_key: "11",
            passenger_identity_key: "26",
            operator_trip_id: "6",
          },
          {
            distance: 5_000,
            driver_identity_key: "11",
            passenger_identity_key: "26",
            operator_trip_id: "6",
          },
          {
            distance: 5_000,
            driver_identity_key: "11",
            passenger_identity_key: "26",
            operator_trip_id: "6",
          },
        ],
        meta: [],
      },
      {
        incentive: [200, 200, 200, 200, 200, 200, 0, 0],
        meta: [
          {
            key: "max_amount_restriction.global.campaign.global",
            value: 1200n,
          },
        ],
      },
    ),
);

it("latest operator", () => {
  const handler = new Handler(100n);
  const { operators } = handler.params();
  assertNotEquals(operators, undefined);
  assertNotEquals(operators, []);
  assertEquals(operators, [OperatorsEnum.BLABLACAR_DAILY]);
});
