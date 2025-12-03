import { it } from "../../../../../dev_deps.ts";
import { v4 as uuidV4 } from "../../../../../lib/uuid/index.ts";
import { OperatorsEnum } from "../../interfaces/index.ts";
import { makeProcessHelper } from "../tests/macro.ts";
import { ATMB202305 as Handler } from "./20230502_ATMB.ts";

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
  operator_class: "C",
  operator_journey_id: uuidV4(),
  operator_id: 1,
  passenger_is_over_18: true,
  passenger_has_travel_pass: true,
  driver_has_travel_pass: true,
  datetime: new Date("2023-05-15"),
  seats: 1,
  distance: 5_000,
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
          { start: { ...defaultPosition, aom: "11" } },
          { end: { ...defaultPosition, aom: "11" } },
          {
            start: {
              ...defaultPosition,
              epci: "200070852", // Usses et Rhône
              aom: "200070852",
            },
            end: {
              ...defaultPosition,
              epci: "200070852",
              aom: "200070852",
            },
            datetime: new Date("2023-12-18"),
          },
          {
            start: {
              ...defaultPosition,
              epci: "247400047", // CC Vallée Verte
              aom: "247400047",
            },
            end: {
              ...defaultPosition,
              epci: "247400047",
              aom: "247400047",
            },
            datetime: new Date("2023-12-18"),
          },
          {
            end: {
              ...defaultPosition,
              aom: "247000623", // CC Quatre Rivière
            },
            start: {
              ...defaultPosition,
              aom: "247000623",
            },
            datetime: new Date("2023-12-18"),
          },
        ],
        meta: [],
      },
      { incentive: [0, 0, 0, 0, 0, 0, 0, 0], meta: [] },
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
            start: {
              ...defaultPosition,
              epci: "200070852", // Usses et Rhône
            },
          },
          { distance: 5_000, seats: 2, driver_identity_key: "one" },
          {
            distance: 25_000,
            driver_identity_key: "one",
            passenger_identity_key: "two",
          },
          {
            distance: 40_000,
            driver_identity_key: "one",
            passenger_identity_key: "three",
          },
          {
            distance: 40_000,
            seats: 2,
            driver_identity_key: "one",
            passenger_identity_key: "three",
          },
          { distance: 60_000, driver_identity_key: "one" },
        ],
        meta: [],
      },
      {
        incentive: [200, 400, 250, 400, 800, 400],
        meta: [
          {
            key: "max_amount_restriction.0-one.month.4-2023",
            value: 2450n,
          },
          {
            key: "max_amount_restriction.global.campaign.global",
            value: 2450n,
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
        carpool: [
          { distance: 5_000, driver_identity_key: "one" },
          { distance: 5_000, driver_identity_key: "one" },
        ],
        meta: [
          {
            key: "max_amount_restriction.0-one.month.4-2023",
            value: 48_00n,
          },
        ],
      },
      {
        incentive: [200, 0],
        meta: [
          {
            key: "max_amount_restriction.0-one.month.4-2023",
            value: 50_00n,
          },
          {
            key: "max_amount_restriction.global.campaign.global",
            value: 200n,
          },
        ],
      },
    ),
);
