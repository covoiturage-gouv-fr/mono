import { it } from "../../../../../dev_deps.ts";
import { v4 as uuidV4 } from "../../../../../lib/uuid/index.ts";
import { OperatorsEnum } from "../../interfaces/index.ts";
import { makeProcessHelper } from "../tests/macro.ts";
import { ATMB2025 as Handler } from "./20251014_ATMB.ts";

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
  operator_uuid: OperatorsEnum.KAROS,
  operator_class: "C",
  operator_journey_id: uuidV4(),
  operator_id: 1,
  passenger_is_over_18: true,
  passenger_has_travel_pass: true,
  driver_has_travel_pass: true,
  datetime: new Date("2025-11-02"),
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

it("should work with exclusion", async () =>
  await process(
    {
      policy: { handler: Handler.id },
      carpool: [
        { operator_uuid: "not in list" },
        { distance: 100 },
        { distance: 25_001 },
        { operator_class: "A" },
        {
          start: {
            ...defaultPosition,
            epci: "200070852", // Usses et Rhône
          },
        },
        {
          end: {
            ...defaultPosition,
            epci: "247400047", // CC Vallée Verte
          },
        },
        {
          end: {
            ...defaultPosition,
            epci: "247000623", // CC Quatre Rivière
          },
        },
      ],
      meta: [],
    },
    { incentive: [0, 0, 0, 0, 0, 0, 0], meta: [] },
  ));

it("should work basic", async () =>
  await process(
    {
      policy: { handler: Handler.id },
      carpool: [
        {
          distance: 5_000,
          driver_identity_key: "one",
        },
        { distance: 5_000, seats: 2, driver_identity_key: "one" },
        {
          distance: 10_000,
          driver_identity_key: "one",
          passenger_identity_key: "two",
        },
        {
          distance: 11_000,
          driver_identity_key: "one",
          passenger_identity_key: "three",
        },
        {
          distance: 20_000,
          driver_identity_key: "one",
          passenger_identity_key: "three",
        },
        {
          distance: 20_000,
          seats: 2,
          driver_identity_key: "one",
          passenger_identity_key: "three",
        },
        { distance: 25_000, driver_identity_key: "one" },
        { distance: 50_000, driver_identity_key: "two" },
      ],
    },
    {
      incentive: [
        150,
        300,
        150,
        140,
        50,
        100,
        0,
        0,
      ],
    },
  ));

it("should work with driver month limits", async () =>
  await process(
    {
      policy: { handler: Handler.id },
      carpool: [
        { distance: 5_000, driver_identity_key: "one" },
        { distance: 5_000, driver_identity_key: "one" },
      ],
      meta: [
        {
          key: "max_amount_restriction.0-one.month.10-2025",
          value: 48_00,
        },
      ],
    },
    {
      incentive: [150, 50],
      meta: [
        {
          key: "max_amount_restriction.0-one.month.10-2025",
          value: 50_00,
        },
        {
          key: "max_amount_restriction.global.campaign.global",
          value: 200,
        },
      ],
    },
  ));
