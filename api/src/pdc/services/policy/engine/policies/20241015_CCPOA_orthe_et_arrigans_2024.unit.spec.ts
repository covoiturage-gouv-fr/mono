import { it } from "dep:testing-bdd";
import { v4 as uuidV4 } from "../../../../../lib/uuid/index.ts";
import { OperatorsEnum } from "../../interfaces/index.ts";
import { makeProcessHelper } from "../tests/macro.ts";
import { CCPOA202410 as Handler } from "./20241015_CCPOA_orthe_et_arrigans_2024.ts";

const defaultPosition = {
  arr: "40231",
  com: "40231",
  aom: "200053759",
  epci: "200069417",
  dep: "40",
  reg: "75",
  country: "XXXXX",
};
const defaultLat = 48.5905360901711;
const defaultLon = 6.499392987670189;

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
  datetime: new Date("2024-11-01"),
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
  end_lat: 48.58685290576798,
  end_lon: 6.483696700766759,
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
      ],
      meta: [],
    },
    {
      incentive: [150, 300, 200, 400],
      meta: [
        {
          key: "max_amount_restriction.global.campaign.global",
          value: 1050n,
        },
        {
          key: "max_amount_restriction.0-one.month.10-2024",
          value: 450n,
        },
        {
          key: "max_amount_restriction.0-two.month.10-2024",
          value: 600n,
        },
      ],
    },
  ));

it("should work with global limits", async () =>
  await process(
    {
      policy: { handler: Handler.id, max_amount: 15_000_00n },
      carpool: [{ distance: 59_000, driver_identity_key: "one" }],
      meta: [
        {
          key: "max_amount_restriction.global.campaign.global",
          value: 14_999_99n,
        },
      ],
    },
    {
      incentive: [1], // <-- should be 250. capped to 1
      meta: [
        {
          key: "max_amount_restriction.global.campaign.global",
          value: 15_000_00n,
        },
        {
          key: "max_amount_restriction.0-one.month.10-2024",
          value: 250n,
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
        { distance: 5_000, driver_identity_key: "one" }, // too much
      ],
      meta: [],
    },
    {
      incentive: [150, 150, 150, 150, 150, 150, 0],
      meta: [
        {
          key: "max_amount_restriction.global.campaign.global",
          value: 900n,
        },
        {
          key: "max_amount_restriction.0-one.month.10-2024",
          value: 900n,
        },
      ],
    },
  ));
