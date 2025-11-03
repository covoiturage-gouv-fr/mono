import { it } from "@/dev_deps.ts";
import { v4 as uuidV4 } from "@/lib/uuid/index.ts";
import { makeProcessHelper } from "@/pdc/services/policy/engine/tests/macro.ts";
import { OperatorsEnum } from "@/pdc/services/policy/interfaces/index.ts";
import { PMGFxATMBxSM4CCx2025 as Handler } from "./20250701_PMGFxATMBxSM4CC.ts";

const defaultPosition = {
  arr: "74206",
  com: "74206",
  aom: "200067551",
  epci: "200067551",
  dep: "74",
  reg: "84",
  country: "XXXXX",
  reseau: 1,
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
  datetime: new Date("2025-07-15"),
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

it("should work with exclusions", async () =>
  await process(
    {
      policy: { handler: Handler.id },
      carpool: [
        { operator_uuid: "not in list" },
        { distance: 100 },
        { operator_class: "A" },
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
        },
      ],
      meta: [],
    },
    { incentive: [0, 0, 0, 0, 0, 0], meta: [] },
  ));

it("trips inside", async () =>
  await process(
    {
      policy: { handler: Handler.id },
      carpool: [
        { distance: 5_000, driver_identity_key: "one" },
        { distance: 5_000, seats: 2, driver_identity_key: "one" },
        {
          distance: 20_000,
          driver_identity_key: "one",
          passenger_identity_key: "two",
        },
        {
          distance: 21_000,
          driver_identity_key: "one",
          passenger_identity_key: "two",
        },
        {
          distance: 30_000,
          driver_identity_key: "one",
          passenger_identity_key: "two",
        },
        {
          distance: 31_000,
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
        {
          distance: 70_000,
          driver_identity_key: "one",
        },
      ],
    },
    {
      incentive: [150, 300, 150, 165, 300, 310, 400, 800, 400],
    },
  ));

it("trips outside AOM before 21/04/2025", async () =>
  await process(
    {
      policy: { handler: Handler.id },
      carpool: [
        {
          distance: 5_000,
          driver_identity_key: "one",
          start: { ...defaultPosition, epci: "200070852", aom: "200070852" },
        },
        {
          distance: 5_000,
          seats: 2,
          driver_identity_key: "one",
          start: { ...defaultPosition, epci: "200070852", aom: "200070852" },
        },
        {
          distance: 20_000,
          driver_identity_key: "one",
          passenger_identity_key: "two",
          start: { ...defaultPosition, epci: "200070852", aom: "200070852" },
        },
        {
          distance: 30_000,
          driver_identity_key: "one",
          passenger_identity_key: "two",
          start: { ...defaultPosition, epci: "200070852", aom: "200070852" },
        },
        {
          distance: 40_000,
          driver_identity_key: "one",
          passenger_identity_key: "three",
          start: { ...defaultPosition, epci: "200070852", aom: "200070852" },
        },
        {
          distance: 40_000,
          seats: 2,
          driver_identity_key: "one",
          passenger_identity_key: "three",
          start: { ...defaultPosition, epci: "200070852", aom: "200070852" },
        },
        {
          distance: 70_000,
          driver_identity_key: "one",
          start: { ...defaultPosition, epci: "200070852", aom: "200070852" },
        },
      ],
    },
    {
      incentive: [50, 100, 50, 175, 300, 600, 300],
    },
  ));

it("trips outside AOM after 21/04/2025", async () =>
  await process(
    {
      policy: { handler: Handler.id },
      carpool: [
        {
          distance: 5_000,
          driver_identity_key: "one",
          start: { ...defaultPosition, epci: "200070852", aom: "200070852" },
          datetime: new Date("2025-07-15"),
        },
        {
          distance: 5_000,
          seats: 2,
          driver_identity_key: "one",
          start: { ...defaultPosition, epci: "200070852", aom: "200070852" },
          datetime: new Date("2025-07-15"),
        },
        {
          distance: 20_000,
          driver_identity_key: "one",
          passenger_identity_key: "two",
          start: { ...defaultPosition, epci: "200070852", aom: "200070852" },
          datetime: new Date("2025-07-15"),
        },
        {
          distance: 21_000,
          driver_identity_key: "one",
          passenger_identity_key: "two",
          start: { ...defaultPosition, epci: "200070852", aom: "200070852" },
          datetime: new Date("2025-07-15"),
        },
        {
          distance: 22_000,
          driver_identity_key: "one",
          passenger_identity_key: "two",
          start: { ...defaultPosition, epci: "200070852", aom: "200070852" },
          datetime: new Date("2025-07-15"),
        },
        {
          distance: 30_000,
          driver_identity_key: "one",
          passenger_identity_key: "two",
          start: { ...defaultPosition, epci: "200070852", aom: "200070852" },
          datetime: new Date("2025-07-15"),
        },
        {
          distance: 40_000,
          driver_identity_key: "one",
          passenger_identity_key: "three",
          start: { ...defaultPosition, epci: "200070852", aom: "200070852" },
          datetime: new Date("2025-07-15"),
        },
        {
          distance: 40_000,
          seats: 2,
          driver_identity_key: "one",
          passenger_identity_key: "three",
          start: { ...defaultPosition, epci: "200070852", aom: "200070852" },
          datetime: new Date("2025-07-15"),
        },
        {
          distance: 70_000,
          driver_identity_key: "one",
          start: { ...defaultPosition, epci: "200070852", aom: "200070852" },
          datetime: new Date("2025-07-15"),
        },
      ],
    },
    {
      incentive: [50, 100, 50, 100, 100, 100, 100, 200, 100],
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
          key: "max_amount_restriction.0-one.month.3-2025",
          value: 48_50,
        },
      ],
    },
    {
      incentive: [150, 0],
      meta: [
        {
          key: "max_amount_restriction.0-one.month.3-2025",
          value: 50_00,
        },
        {
          key: "max_amount_restriction.global.campaign.global",
          value: 150,
        },
      ],
    },
  ));
