import { it } from "dep:testing-bdd";
import { v4 as uuidV4 } from "@/lib/uuid/index.ts";
import { OperatorsEnum } from "../../interfaces/index.ts";
import { generatePartialCarpools } from "../tests/helpers.ts";
import { makeProcessHelper } from "../tests/macro.ts";
import { GrandPoitiers2027 as Handler } from "./20270101_GrandPoitiers2027.ts";

// Unit test calculations

const defaultPosition = {
  arr: "86194",
  com: "86194",
  aom: "200069854",
  epci: "200069854",
  dep: "86",
  reg: "75",
  country: "XXXXX",
};
const defaultLat = 46.575171256996086;
const defaultLon = 0.36385131640839713;

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
  datetime: new Date("2027-01-15"),
  seats: 1,
  distance: 7_000,
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

it("should work with exclusions", async () =>
  await process(
    {
      policy: { handler: Handler.id },
      carpool: [
        { operator_uuid: "not in list" },
        { distance: 100 },
        { operator_class: "A" },
        { operator_class: "B" },
        { distance: 81_000 },
        { passenger_is_over_18: false },
        { datetime: new Date("2026-12-31T22:59:00Z") },
      ],
    },
    { incentive: [0, 0, 0, 0, 0, 0, 0] },
  ));

it("should work with distance bounds", async () =>
  await process(
    {
      policy: { handler: Handler.id },
      carpool: [
        { distance: 4_998 },
        { distance: 4_999 },
        { distance: 5_000 },
        { distance: 80_000 },
        { distance: 80_001 },
      ],
    },
    { incentive: [0, 0, 150, 150, 0] },
  ));

it("should work with period bounds", async () =>
  await process(
    {
      policy: { handler: Handler.id },
      carpool: [
        { datetime: new Date("2027-01-01T00:00:00+0100") },
        { datetime: new Date("2027-12-31T23:59:00+0100") },
      ],
    },
    { incentive: [150, 150] },
  ));

it("should work basic", async () =>
  await process(
    {
      policy: { handler: Handler.id },
      carpool: [
        { distance: 5_000, driver_identity_key: "one" },
        {
          distance: 79_999,
          seats: 2,
          driver_identity_key: "one",
          passenger_identity_key: "three",
        },
      ],
    },
    {
      incentive: [150, 300],
    },
  ));

it("should work with driver month limits", async () =>
  await process(
    {
      policy: { handler: Handler.id },
      carpool: [
        { distance: 6_000, driver_identity_key: "one" },
        { distance: 6_000, driver_identity_key: "one" },
      ],
      meta: [
        {
          key: "max_amount_restriction.0-one.month.0-2027",
          value: 119_00,
        },
        {
          key: "max_amount_restriction.global.campaign.global",
          value: 119_00,
        },
      ],
    },
    {
      incentive: [100, 0],
      meta: [
        {
          key: "max_amount_restriction.0-one.month.0-2027",
          value: 120_00,
        },
        {
          key: "max_amount_restriction.global.campaign.global",
          value: 120_00,
        },
      ],
    },
  ));

it("should work with driver day limits", async () =>
  await process(
    {
      policy: { handler: Handler.id },
      carpool: [
        {
          distance: 6_000,
          driver_identity_key: "11",
          passenger_identity_key: "21",
          operator_trip_id: "1",
        },
        {
          distance: 6_000,
          driver_identity_key: "11",
          passenger_identity_key: "22",
          operator_trip_id: "2",
        },
        {
          distance: 6_000,
          driver_identity_key: "11",
          passenger_identity_key: "23",
          operator_trip_id: "3",
        },
        {
          distance: 6_000,
          driver_identity_key: "11",
          passenger_identity_key: "24",
          operator_trip_id: "4",
        },
        {
          distance: 6_000,
          driver_identity_key: "11",
          passenger_identity_key: "25",
          operator_trip_id: "5",
        },
        {
          distance: 6_000,
          driver_identity_key: "11",
          passenger_identity_key: "26",
          operator_trip_id: "6",
        },
        {
          distance: 6_000,
          driver_identity_key: "11",
          passenger_identity_key: "26",
          operator_trip_id: "6",
        },
      ],
      meta: [],
    },
    {
      incentive: [150, 150, 150, 150, 150, 150, 0],
      meta: [
        {
          key: "max_amount_restriction.0-11.month.0-2027",
          value: 900,
        },
        {
          key: "max_amount_restriction.global.campaign.global",
          value: 900,
        },
      ],
    },
  ));

it("should work with passenger day limits", async () =>
  await process(
    {
      policy: { handler: Handler.id },
      carpool: [
        {
          distance: 6_000,
          driver_identity_key: "11",
          passenger_identity_key: "21",
          operator_trip_id: "1",
        },
        {
          distance: 6_000,
          driver_identity_key: "11",
          passenger_identity_key: "21",
          operator_trip_id: "2",
        },
        {
          distance: 6_000,
          driver_identity_key: "11",
          passenger_identity_key: "21",
          operator_trip_id: "3",
        },
      ],
      meta: [],
    },
    {
      incentive: [150, 150, 0],
      meta: [
        {
          key: "max_amount_restriction.0-11.month.0-2027",
          value: 300,
        },
        {
          key: "max_amount_restriction.global.campaign.global",
          value: 300,
        },
      ],
    },
  ));

it("should work with driver amount month limits", async () =>
  await process(
    {
      policy: { handler: Handler.id },
      carpool: generatePartialCarpools(80, new Date("2027-01-01")),
      meta: [],
    },
    {
      incentive: [...[...Array(80).keys()].map(() => 150), 0],
      meta: [
        {
          key: "max_amount_restriction.0-three.month.0-2027",
          value: 120_00,
        },
        {
          key: "max_amount_restriction.global.campaign.global",
          value: 120_00,
        },
      ],
    },
  ));
