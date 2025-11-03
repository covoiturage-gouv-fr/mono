import { it } from "../../../../../dev_deps.ts";
import { v4 as uuidV4 } from "../../../../../lib/uuid/index.ts";
import { OperatorsEnum } from "../../interfaces/index.ts";
import { makeProcessHelper } from "../tests/macro.ts";
import { HauteCorreze2026 as Handler } from "./20250902_haute_correze_2026.ts";

const defaultPosition = {
  arr: "19139",
  com: "19139",
  aom: "200066744",
  epci: "200066744",
  dep: "19",
  reg: "75",
  country: "XXXXX",
  reseau: 73,
};
const defaultLat = 45.54858380899173;
const defaultLon = 2.3146017323921195;

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
  datetime: new Date("2025-11-01"),
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
  end_lat: 45.53725299731382,
  end_lon: 2.3059201226861297,
};

const process = makeProcessHelper(defaultCarpool);

it("should work with exclusions", async () =>
  await process(
    {
      policy: { handler: Handler.id },
      carpool: [
        { operator_uuid: "not in list" },
        { operator_uuid: OperatorsEnum.KLAXIT },
        { distance: 100 },
        { distance: 90_000 },
        { operator_class: "A" },
      ],
      meta: [],
    },
    { incentive: [0, 0, 0, 0, 0], meta: [] },
  ));

it("should work basic with start/end inside aom", async () =>
  await process(
    {
      policy: { handler: Handler.id },
      carpool: [
        { distance: 5_000, driver_identity_key: "one" },
        { distance: 5_000, seats: 2, driver_identity_key: "one" },
        { distance: 25_000, driver_identity_key: "two" },
        { distance: 25_000, seats: 2, driver_identity_key: "two" },
        { distance: 80_000, driver_identity_key: "marcel" },
      ],
      meta: [],
    },
    {
      incentive: [150, 300, 250, 500, 300],
      meta: [
        {
          key: "max_amount_restriction.global.campaign.global",
          value: 1500,
        },
        {
          key: "max_amount_restriction.0-one.month.10-2025",
          value: 450,
        },
        {
          key: "max_amount_restriction.0-two.month.10-2025",
          value: 750,
        },
        {
          key: "max_amount_restriction.0-marcel.month.10-2025",
          value: 300,
        },
      ],
    },
  ));

it("should work with global limits", async () =>
  await process(
    {
      policy: { handler: Handler.id, max_amount: 15_000_00 },
      carpool: [{ distance: 79_000, driver_identity_key: "one" }],
      meta: [
        {
          key: "max_amount_restriction.global.campaign.global",
          value: 14_999_99,
        },
      ],
    },
    {
      incentive: [1], // <-- should be 250. capped to 1
      meta: [
        {
          key: "max_amount_restriction.global.campaign.global",
          value: 15_000_00,
        },
        {
          key: "max_amount_restriction.0-one.month.10-2025",
          value: 300,
        },
      ],
    },
  ));

it("should work with 6 trips per day limit", async () =>
  await process(
    {
      policy: { handler: Handler.id },
      carpool: [
        { distance: 5_000, driver_identity_key: "one" },
        { distance: 5_000, driver_identity_key: "one" },
        { distance: 5_000, driver_identity_key: "one" },
        { distance: 5_000, driver_identity_key: "one" },
        { distance: 5_000, driver_identity_key: "one" },
        { distance: 5_000, driver_identity_key: "one" },
        { distance: 5_000, driver_identity_key: "one" }, // too many
      ],
      meta: [],
    },
    {
      incentive: [150, 150, 150, 150, 150, 150, 0],
      meta: [
        {
          key: "max_amount_restriction.global.campaign.global",
          value: 900,
        },
        {
          key: "max_amount_restriction.0-one.month.10-2025",
          value: 900,
        },
      ],
    },
  ));
