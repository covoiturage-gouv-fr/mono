import { it } from "dep:testing-bdd";
import { v4 as uuidV4 } from "../../../../../lib/uuid/index.ts";
import { OperatorsEnum } from "../../interfaces/index.ts";
import { makeProcessHelper } from "../tests/macro.ts";
import { LaRochelle2024 as Handler } from "./20240101_LaRochelle.ts";

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
  driver_identity_key: "driver_id_one",
  operator_uuid: OperatorsEnum.BLABLACAR_DAILY,
  operator_class: "C",
  passenger_is_over_18: true,
  passenger_has_travel_pass: true,
  driver_has_travel_pass: true,
  datetime: new Date("2024-01-02"),
  seats: 1,
  distance: 6_000,
  operator_journey_id: uuidV4(),
  operator_id: 9,
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

it("should work with exclusions", async () =>
  await process(
    {
      policy: { handler: Handler.id },
      carpool: [
        { operator_uuid: OperatorsEnum.KLAXIT },
        { distance: 100 },
        { distance: 4_000 },
        { operator_class: "A" },
      ],
      meta: [],
    },
    {
      incentive: [0, 0, 0, 0],
      meta: [],
    },
  ));

it("should work basic", async () =>
  await process(
    {
      policy: { handler: Handler.id },
      carpool: [
        { distance: 2_000 },
        { distance: 6_000 },
        { distance: 6_000, seats: 2 },
        { distance: 80_000 },
        { distance: 80_000, seats: 2 },
      ],
      meta: [],
    },
    {
      incentive: [0, 100, 200, 200, 400],
      meta: [
        {
          key: "max_amount_restriction.global.campaign.global",
          value: 900,
        },
        {
          key: "max_amount_restriction.0-driver_id_one.month.0-2024",
          value: 900,
        },
      ],
    },
  ));

it("should work with driver month limits of 80 €", async () =>
  await process(
    {
      policy: { handler: Handler.id },
      carpool: [
        { distance: 6_000 },
        { distance: 6_000 },
      ],
      meta: [
        {
          key: "max_amount_restriction.0-driver_id_one.month.0-2024",
          value: 79_00,
        },
        {
          key: "max_amount_restriction.global.campaign.global",
          value: 79_00,
        },
      ],
    },
    {
      incentive: [100, 0],
      meta: [
        {
          key: "max_amount_restriction.global.campaign.global",
          value: 80_00,
        },
        {
          key: "max_amount_restriction.0-driver_id_one.month.0-2024",
          value: 80_00,
        },
      ],
    },
  ));
