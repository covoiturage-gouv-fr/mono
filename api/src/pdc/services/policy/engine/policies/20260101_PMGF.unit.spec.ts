import { it } from "dep:testing-bdd";
import { v4 as uuidV4 } from "../../../../../lib/uuid/index.ts";
import { OperatorsEnum } from "../../interfaces/index.ts";
import { makeProcessHelper } from "../tests/macro.ts";
import { PMGF2026 as Handler } from "./20260101_PMGF.ts";

const defaultPosition = {
  arr: "74206",
  com: "74206",
  aom: "200067551",
  epci: "200067551",
  dep: "74",
  reg: "84",
  country: "XXXXX",
};

const defaultLat = 46.313355215729146;
const defaultLon = 6.487631441991693;

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
  datetime: new Date("2026-03-15"),
  seats: 1,
  duration: 600,
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

// Position outside the PMGF territory but in France
const outsideFrancePosition = {
  arr: "69123",
  com: "69123",
  aom: "200046977",
  epci: "200046977",
  dep: "69",
  reg: "84",
  country: "XXXXX",
};

// Position in Switzerland
const switzerlandPosition = {
  arr: "99140",
  com: "",
  aom: "",
  epci: "",
  dep: "",
  reg: "",
  country: "99140",
};

it("should work with exclusions", async () =>
  await process(
    {
      policy: { handler: Handler.id },
      carpool: [
        // Wrong operator
        { operator_uuid: "not in list" },
        // Too short
        { distance: 100 },
        // Too long
        { distance: 100_000 },
        // Wrong class
        { operator_class: "A" },
        // Both ends outside territory
        {
          start: { ...outsideFrancePosition },
          end: { ...outsideFrancePosition },
        },
      ],
      meta: [],
    },
    { incentive: [0, 0, 0, 0, 0], meta: [] },
  ));

it("internal trips (start AND end in territory)", async () =>
  await process(
    {
      policy: { handler: Handler.id },
      carpool: [
        // 5km -> 1.50 EUR
        { distance: 5_000, driver_identity_key: "one" },
        // 5km, 2 seats -> 3.00 EUR
        { distance: 5_000, seats: 2, driver_identity_key: "one" },
        // 20km -> 1.50 EUR
        { distance: 20_000, driver_identity_key: "one", passenger_identity_key: "two" },
        // 21km -> 1.50 + 0.05*1 = 1.55 EUR
        { distance: 21_000, driver_identity_key: "one", passenger_identity_key: "two" },
        // 30km -> 1.50 + 0.05*10 = 2.00 EUR
        { distance: 30_000, driver_identity_key: "one", passenger_identity_key: "two" },
        // 35km -> 2.00 EUR (flat 30-40km)
        { distance: 35_000, driver_identity_key: "one", passenger_identity_key: "two" },
        // Use a different driver to avoid daily 6-trip limit
        // 40km -> 2.00 EUR
        { distance: 40_000, driver_identity_key: "two", passenger_identity_key: "three" },
        // 45km -> 2.00 - 0.10*5 = 1.50 EUR
        { distance: 45_000, driver_identity_key: "two", passenger_identity_key: "three" },
        // 50km -> 2.00 - 0.10*10 = 1.00 EUR
        { distance: 50_000, driver_identity_key: "two", passenger_identity_key: "three" },
        // 70km -> 1.00 EUR (capped)
        { distance: 70_000, driver_identity_key: "two" },
      ],
    },
    {
      incentive: [150, 300, 150, 155, 200, 200, 200, 150, 100, 100],
    },
  ));

it("external France trips (one end outside territory)", async () =>
  await process(
    {
      policy: { handler: Handler.id },
      carpool: [
        // 5km -> 0.50 EUR
        {
          distance: 5_000,
          driver_identity_key: "one",
          start: { ...outsideFrancePosition },
        },
        // 5km, 2 seats -> 1.00 EUR
        {
          distance: 5_000,
          seats: 2,
          driver_identity_key: "one",
          start: { ...outsideFrancePosition },
        },
        // 20km -> 0.50 EUR
        {
          distance: 20_000,
          driver_identity_key: "one",
          passenger_identity_key: "two",
          start: { ...outsideFrancePosition },
        },
        // 25km -> 0.50 + 0.50 = 1.00 EUR
        {
          distance: 25_000,
          driver_identity_key: "one",
          passenger_identity_key: "two",
          start: { ...outsideFrancePosition },
        },
        // 60km -> 1.00 EUR (capped)
        {
          distance: 60_000,
          driver_identity_key: "one",
          passenger_identity_key: "three",
          start: { ...outsideFrancePosition },
        },
      ],
    },
    {
      incentive: [50, 100, 100, 100, 100],
    },
  ));

it("Switzerland trips (one end in Switzerland)", async () =>
  await process(
    {
      policy: { handler: Handler.id },
      carpool: [
        // 5km -> 0.50 EUR flat
        {
          distance: 5_000,
          driver_identity_key: "one",
          start: { ...switzerlandPosition },
        },
        // 30km -> 0.50 EUR flat
        {
          distance: 30_000,
          driver_identity_key: "one",
          passenger_identity_key: "two",
          start: { ...switzerlandPosition },
        },
        // 70km -> 0.50 EUR flat
        {
          distance: 70_000,
          driver_identity_key: "one",
          passenger_identity_key: "two",
          start: { ...switzerlandPosition },
        },
        // 30km, 2 seats -> 1.00 EUR
        {
          distance: 30_000,
          seats: 2,
          driver_identity_key: "one",
          passenger_identity_key: "three",
          start: { ...switzerlandPosition },
        },
      ],
    },
    {
      incentive: [50, 50, 50, 100],
    },
  ));

it("new EPCI 200069730 (CC des 4 Rivieres) is in territory", async () =>
  await process(
    {
      policy: { handler: Handler.id },
      carpool: [
        // Internal trip within new EPCI -> internal rate
        {
          distance: 5_000,
          driver_identity_key: "one",
          start: { ...defaultPosition, epci: "200069730", aom: "200069730" },
          end: { ...defaultPosition, epci: "200069730", aom: "200069730" },
        },
        // External trip from new EPCI to outside France -> external France rate
        {
          distance: 25_000,
          driver_identity_key: "one",
          passenger_identity_key: "two",
          start: { ...outsideFrancePosition },
          end: { ...defaultPosition, epci: "200069730", aom: "200069730" },
        },
      ],
    },
    {
      incentive: [150, 100],
    },
  ));

it("should enforce driver daily trip limit (6 trips/day)", async () =>
  await process(
    {
      policy: { handler: Handler.id },
      carpool: [
        { distance: 5_000, driver_identity_key: "driver1" },
        { distance: 5_000, driver_identity_key: "driver1" },
        { distance: 5_000, driver_identity_key: "driver1" },
        { distance: 5_000, driver_identity_key: "driver1" },
        { distance: 5_000, driver_identity_key: "driver1" },
        { distance: 5_000, driver_identity_key: "driver1" },
        // 7th trip should be blocked
        { distance: 5_000, driver_identity_key: "driver1" },
      ],
    },
    {
      incentive: [150, 150, 150, 150, 150, 150, 0],
    },
  ));

it("should enforce driver monthly amount limit (50 EUR)", async () =>
  await process(
    {
      policy: { handler: Handler.id },
      carpool: [
        { distance: 5_000, driver_identity_key: "one" },
        { distance: 5_000, driver_identity_key: "one" },
      ],
      meta: [
        {
          key: "max_amount_restriction.0-one.month.2-2026",
          value: 48_50n,
        },
      ],
    },
    {
      incentive: [150, 0],
      meta: [
        {
          key: "max_amount_restriction.0-one.month.2-2026",
          value: 50_00n,
        },
        {
          key: "max_amount_restriction.global.campaign.global",
          value: 150n,
        },
      ],
    },
  ));
