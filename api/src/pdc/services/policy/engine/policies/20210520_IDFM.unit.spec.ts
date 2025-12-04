import { describe, it } from "../../../../../dev_deps.ts";
import { v4 as uuidV4 } from "../../../../../lib/uuid/index.ts";
import { OperatorsEnum } from "../../interfaces/index.ts";
import { makeProcessHelper } from "../tests/macro.ts";
import { IDFMPeriodeNormale2021 as Handler } from "./20210520_IDFM.ts";

describe("IDFM campaign", () => {
  const defaultPosition = {
    arr: "91377",
    com: "91377",
    aom: "217500016",
    epci: "200056232",
    dep: "91",
    reg: "11",
    country: "XXXXX",
    reseau: 232,
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
    passenger_is_over_18: true,
    passenger_has_travel_pass: true,
    driver_has_travel_pass: true,
    datetime: new Date("2021-05-21"),
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
    end_lat: defaultLat,
    end_lon: defaultLon,
  };

  const process = makeProcessHelper(defaultCarpool);

  it(
    "should work with exclusions",
    async () =>
      await process(
        {
          policy: { handler: Handler.id },
          carpool: [
            { operator_uuid: "not in list" },
            { distance: 100 },
            { distance: 200_000 },
            {
              start: { ...defaultPosition, com: "75056" },
              end: { ...defaultPosition, com: "75056" },
            },
            { start: { ...defaultPosition, aom: "not_ok" } },
            { end: { ...defaultPosition, aom: "not_ok" } },
            { operator_class: "A" },
            { operator_class: "B", datetime: new Date("2022-09-02") },
            { operator_uuid: OperatorsEnum.YNSTANT },
          ],
          meta: [],
        },
        { incentive: [0, 0, 0, 0, 0, 0, 0, 0, 0], meta: [] },
      ),
  );

  it(
    "should work basic",
    async () =>
      await process(
        {
          policy: { handler: Handler.id },
          carpool: [
            { distance: 5_000, driver_identity_key: "one" },
            { distance: 5_000, seats: 2, driver_identity_key: "one" },
            { distance: 25_000, driver_identity_key: "two" },
            {
              distance: 25_000,
              driver_identity_key: "two",
              datetime: new Date("2022-03-28"),
            },
            { distance: 55_000, driver_identity_key: "three" },
            {
              distance: 5_000,
              operator_uuid: OperatorsEnum.YNSTANT,
              datetime: new Date("2023-03-22T00:00:00+0100"),
              driver_identity_key: "four",
            },
            {
              distance: 5_000,
              driver_identity_key: "four",
              start: { ...defaultPosition, aom: "287500078" },
              end: { ...defaultPosition, aom: "287500078" },
            },
          ],
          meta: [],
        },
        {
          incentive: [150, 300, 250, 375, 300, 150, 150],
          meta: [
            {
              key: "max_amount_restriction.0-one.month.4-2021",
              value: 450n,
            },
            {
              key: "max_amount_restriction.global.campaign.global",
              value: 1675n,
            },
            {
              key: "max_amount_restriction.0-two.month.4-2021",
              value: 250n,
            },
            {
              key: "max_amount_restriction.0-two.month.2-2022",
              value: 375n,
            },
            {
              key: "max_amount_restriction.0-three.month.4-2021",
              value: 300n,
            },
            {
              key: "max_amount_restriction.0-four.month.2-2023",
              value: 150n,
            },
            {
              key: "max_amount_restriction.0-four.month.4-2021",
              value: 150n,
            },
          ],
        },
      ),
  );

  it(
    "should work on special tarification days",
    async () =>
      await process(
        {
          policy: { handler: Handler.id },
          carpool: [
            {
              distance: 23_920,
              driver_identity_key: "1",
              datetime: new Date("2023-02-07"),
            },
            {
              distance: 31_664,
              driver_identity_key: "2",
              datetime: new Date("2023-02-07"),
            },
            {
              distance: 43_373,
              driver_identity_key: "3",
              datetime: new Date("2023-02-07"),
            },
            {
              distance: 13_799,
              driver_identity_key: "4",
              datetime: new Date("2023-02-07"),
            },
            {
              distance: 6_306,
              driver_identity_key: "5",
              datetime: new Date("2023-02-07"),
            },
            {
              distance: 6_306,
              driver_identity_key: "6",
              datetime: new Date("2024-12-11"),
              start: { ...defaultPosition, com: "78172" },
            },
            {
              distance: 30_000,
              driver_identity_key: "6",
              datetime: new Date("2025-03-15"),
              start: { ...defaultPosition, com: "95323" },
            },
          ],
          meta: [],
        },
        {
          incentive: [
            Math.ceil(239 * 1.5),
            300 * 1.5,
            300 * 1.5,
            150 * 1.5,
            150 * 1.5,
            150 * 1.5,
            300 * 1.5,
          ],
          meta: [
            {
              key: "max_amount_restriction.0-1.month.1-2023",
              value: 359n,
            },
            {
              key: "max_amount_restriction.global.campaign.global",
              value: 2384n,
            },
            {
              key: "max_amount_restriction.0-2.month.1-2023",
              value: 450n,
            },
            {
              key: "max_amount_restriction.0-3.month.1-2023",
              value: 450n,
            },
            {
              key: "max_amount_restriction.0-4.month.1-2023",
              value: 225n,
            },
            {
              key: "max_amount_restriction.0-5.month.1-2023",
              value: 225n,
            },
            {
              key: "max_amount_restriction.0-6.month.11-2024",
              value: 225n,
            },
            {
              key: "max_amount_restriction.0-6.month.2-2025",
              value: 450n,
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
          policy: { handler: Handler.id, max_amount: 10_300_000_00n },
          carpool: [{ distance: 5_000, driver_identity_key: "one" }],
          meta: [
            {
              key: "max_amount_restriction.global.campaign.global",
              value: 10_299_999_50n,
            },
          ],
        },
        {
          incentive: [50],
          meta: [
            {
              key: "max_amount_restriction.0-one.month.4-2021",
              value: 150n,
            },
            {
              key: "max_amount_restriction.global.campaign.global",
              value: 10_300_000_00n,
            },
          ],
        },
      ),
  );

  it(
    "should work with month limits",
    async () =>
      await process(
        {
          policy: { handler: Handler.id },
          carpool: [{ distance: 5_000, driver_identity_key: "one" }],
          meta: [
            {
              key: "max_amount_restriction.0-one.month.4-2021",
              value: 149_00n,
            },
          ],
        },
        {
          incentive: [100],
          meta: [
            {
              key: "max_amount_restriction.0-one.month.4-2021",
              value: 150_00n,
            },
            {
              key: "max_amount_restriction.global.campaign.global",
              value: 100n,
            },
          ],
        },
      ),
  );

  it(
    "should work with day limits",
    async () =>
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
            { distance: 5_000, driver_identity_key: "one" },
          ],
          meta: [],
        },
        {
          incentive: [150, 150, 150, 150, 150, 150, 0],
          meta: [
            {
              key: "max_amount_restriction.0-one.month.4-2021",
              value: 900n,
            },
            {
              key: "max_amount_restriction.global.campaign.global",
              value: 900n,
            },
          ],
        },
      ),
  );

  /**
   * Check operators entry dates
   */

  it(
    "check BLABLACAR_DAILY entry dates",
    async () =>
      await process(
        {
          policy: { handler: Handler.id },
          carpool: [
            {
              datetime: new Date("2017-05-20T00:00:00+0200"),
              operator_uuid: OperatorsEnum.BLABLACAR_DAILY,
              driver_identity_key: uuidV4(),
              passenger_identity_key: uuidV4(),
            },
            {
              datetime: new Date("2021-05-20T00:00:00+0200"),
              operator_uuid: OperatorsEnum.BLABLACAR_DAILY,
              driver_identity_key: uuidV4(),
              passenger_identity_key: uuidV4(),
            },
            {
              datetime: new Date("2023-01-01T00:00:00+0100"),
              operator_uuid: OperatorsEnum.BLABLACAR_DAILY,
              driver_identity_key: uuidV4(),
              passenger_identity_key: uuidV4(),
            },
            {
              datetime: new Date("2023-03-22T00:00:00+0100"),
              operator_uuid: OperatorsEnum.BLABLACAR_DAILY,
              driver_identity_key: uuidV4(),
              passenger_identity_key: uuidV4(),
            },
          ],
        },
        { incentive: [0, 150, 150, 150] },
      ),
  );

  it(
    "check KAROS entry dates",
    async () =>
      await process(
        {
          policy: { handler: Handler.id },
          carpool: [
            {
              datetime: new Date("2017-05-20T00:00:00+0200"),
              operator_uuid: OperatorsEnum.KAROS,
              driver_identity_key: uuidV4(),
              passenger_identity_key: uuidV4(),
            },
            {
              datetime: new Date("2021-05-20T00:00:00+0200"),
              operator_uuid: OperatorsEnum.KAROS,
              driver_identity_key: uuidV4(),
              passenger_identity_key: uuidV4(),
            },
            {
              datetime: new Date("2023-01-01T00:00:00+0100"),
              operator_uuid: OperatorsEnum.KAROS,
              driver_identity_key: uuidV4(),
              passenger_identity_key: uuidV4(),
            },
            {
              datetime: new Date("2023-03-22T00:00:00+0100"),
              operator_uuid: OperatorsEnum.KAROS,
              driver_identity_key: uuidV4(),
              passenger_identity_key: uuidV4(),
            },
          ],
        },
        { incentive: [0, 150, 150, 150] },
      ),
  );

  it(
    "check KLAXIT entry dates",
    async () =>
      await process(
        {
          policy: { handler: Handler.id },
          carpool: [
            {
              datetime: new Date("2017-05-20T00:00:00+0200"),
              operator_uuid: OperatorsEnum.KLAXIT,
              driver_identity_key: uuidV4(),
              passenger_identity_key: uuidV4(),
            },
            {
              datetime: new Date("2021-05-20T00:00:00+0200"),
              operator_uuid: OperatorsEnum.KLAXIT,
              driver_identity_key: uuidV4(),
              passenger_identity_key: uuidV4(),
            },
            {
              datetime: new Date("2023-01-01T00:00:00+0100"),
              operator_uuid: OperatorsEnum.KLAXIT,
              driver_identity_key: uuidV4(),
              passenger_identity_key: uuidV4(),
            },
            {
              datetime: new Date("2023-03-22T00:00:00+0100"),
              operator_uuid: OperatorsEnum.KLAXIT,
              driver_identity_key: uuidV4(),
              passenger_identity_key: uuidV4(),
            },
          ],
        },
        { incentive: [0, 150, 150, 150] },
      ),
  );

  it(
    "check MOBICOOP entry dates",
    async () =>
      await process(
        {
          policy: { handler: Handler.id },
          carpool: [
            {
              datetime: new Date("2023-01-01T00:00:00+0100"),
              operator_uuid: OperatorsEnum.MOBICOOP,
              driver_identity_key: uuidV4(),
              passenger_identity_key: uuidV4(),
            },
            {
              datetime: new Date("2023-03-22T00:00:00+0100"),
              operator_uuid: OperatorsEnum.MOBICOOP,
              driver_identity_key: uuidV4(),
              passenger_identity_key: uuidV4(),
            },
          ],
        },
        { incentive: [0, 150] },
      ),
  );

  it(
    "check YNSTANT entry dates",
    async () =>
      await process(
        {
          policy: { handler: Handler.id },
          carpool: [
            {
              datetime: new Date("2021-05-20T00:00:00+0200"),
              operator_uuid: OperatorsEnum.YNSTANT,
              driver_identity_key: uuidV4(),
              passenger_identity_key: uuidV4(),
            },
            {
              datetime: new Date("2023-01-01T00:00:00+0100"),
              operator_uuid: OperatorsEnum.YNSTANT,
              driver_identity_key: uuidV4(),
              passenger_identity_key: uuidV4(),
            },
            {
              datetime: new Date("2023-03-22T00:00:00+0100"),
              operator_uuid: OperatorsEnum.YNSTANT,
              driver_identity_key: uuidV4(),
              passenger_identity_key: uuidV4(),
            },
          ],
        },
        { incentive: [0, 150, 150] },
      ),
  );
});
