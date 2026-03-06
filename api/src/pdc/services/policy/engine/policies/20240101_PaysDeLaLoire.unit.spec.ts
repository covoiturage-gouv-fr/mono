import sinon from "dep:sinon";
import { describe, it } from "@/dev_deps.ts";
import { v4 as uuidV4 } from "@/lib/uuid/index.ts";
import { OperatorsEnum, TerritoryCodeInterface } from "../../interfaces/index.ts";
import { makeProcessHelper } from "../tests/macro.ts";
import { PaysDeLaLoire2024 as Handler } from "./20240101_PaysDeLaLoire.ts";

describe("PaysDeLaLoire2024", () => {
  const defaultPosition: TerritoryCodeInterface = {
    arr: "85047",
    com: "85047",
    aom: "200071629",
    epci: "200071629",
    dep: "85",
    reg: "52",
    country: "XXXXX",
    reseau: 1,
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
    datetime: new Date("2024-04-15"),
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

  sinon.stub(Handler, "boosterDates").get(() => ["2024-04-16"]);

  const process = makeProcessHelper(defaultCarpool);

  it("should work with regular exclusions", async () =>
    await process({
      policy: { handler: Handler.id },
      carpool: [
        { distance: 4999 },
        { operator_class: "A" },
        { start: { ...defaultPosition, reg: "11" } },
        { end: { ...defaultPosition, reg: "11" } },
        { distance: 60_001 },
        { passenger_is_over_18: false },
      ],
    }, { incentive: [0, 0, 0, 0, 0, 0] }));

  it("Klaxit removed on 2024-03-18", async () =>
    await process({
      policy: { handler: Handler.id },
      carpool: [
        {
          operator_uuid: OperatorsEnum.KLAXIT,
          datetime: new Date("2024-03-17T23:00:00+0100"),
        },
        {
          operator_uuid: OperatorsEnum.KLAXIT,
          datetime: new Date("2024-03-18T10:00:00+0100"),
        },
      ],
    }, { incentive: [75, 0] }));

  it("should work with AOM exclusions", async () =>
    await process({
      policy: { handler: Handler.id },
      carpool: [
        // Nantes Métropole (244400404)
        {
          driver_identity_key: "nantes",
          start: { ...defaultPosition, aom: "244400404" },
          end: { ...defaultPosition, aom: "244400404" },
        },
        {
          driver_identity_key: "nantes",
          start: { ...defaultPosition, aom: "244400404" },
          end: { ...defaultPosition },
        },
        {
          driver_identity_key: "nantes",
          start: { ...defaultPosition },
          end: { ...defaultPosition, aom: "244400404" },
        },

        // Angers (244900015)
        {
          driver_identity_key: "angers",
          start: { ...defaultPosition, aom: "244900015" },
          end: { ...defaultPosition, aom: "244900015" },
        },
        {
          driver_identity_key: "angers",
          start: { ...defaultPosition, aom: "244900015" },
          end: { ...defaultPosition },
        },
        {
          driver_identity_key: "angers",
          start: { ...defaultPosition },
          end: { ...defaultPosition, aom: "244900015" },
        },

        // Le Mans (247200132)
        {
          driver_identity_key: "le_mans",
          start: { ...defaultPosition, aom: "247200132" },
          end: { ...defaultPosition, aom: "247200132" },
        },
        {
          driver_identity_key: "le_mans",
          start: { ...defaultPosition, aom: "247200132" },
          end: { ...defaultPosition },
        },
        {
          driver_identity_key: "le_mans",
          start: { ...defaultPosition },
          end: { ...defaultPosition, aom: "247200132" },
        },

        // CA Agglomération du Choletais (200071678)
        {
          driver_identity_key: "cholet",
          start: { ...defaultPosition, aom: "200071678" },
          end: { ...defaultPosition, aom: "200071678" },
        },
        {
          driver_identity_key: "cholet",
          start: { ...defaultPosition, aom: "200071678" },
          end: { ...defaultPosition },
        },
        {
          driver_identity_key: "cholet",
          start: { ...defaultPosition },
          end: { ...defaultPosition, aom: "200071678" },
        },
      ],
    }, { incentive: [0, 75, 75, 0, 75, 75, 0, 75, 75, 0, 75, 75] }));

  it("should work basic", async () =>
    await process({
      policy: { handler: Handler.id },
      carpool: [
        { distance: 1_000, driver_identity_key: "one" },
        { distance: 5_000, driver_identity_key: "one" },
        { distance: 5_000, seats: 2, driver_identity_key: "one" },
        { distance: 20_000, driver_identity_key: "two" },
        { distance: 25_000, driver_identity_key: "two" },
        { distance: 55_000, driver_identity_key: "two" },
        { distance: 61_000, driver_identity_key: "two" },
      ],
      meta: [],
    }, {
      incentive: [0, 75, 150, 105, 155, 200, 0],
      meta: [
        {
          key: "max_amount_restriction.global.campaign.global",
          value: 685n,
        },
        {
          key: "max_amount_restriction.0-one.month.3-2024",
          value: 225n,
        },
        {
          key: "max_amount_restriction.0-one.year.2024",
          value: 225n,
        },
        {
          key: "max_amount_restriction.0-two.month.3-2024",
          value: 460n,
        },
        {
          key: "max_amount_restriction.0-two.year.2024",
          value: 460n,
        },
      ],
    }));

  it("should apply booster rules on booster dates", async () =>
    await process({
      policy: { handler: Handler.id },
      carpool: [
        { distance: 6_000, datetime: new Date("2024-04-01") },
        { distance: 6_000, datetime: new Date("2024-04-16") },
        { distance: 20_000, datetime: new Date("2024-04-01") },
        { distance: 20_000, datetime: new Date("2024-04-16") },
        { distance: 55_000, datetime: new Date("2024-04-01") },
        { distance: 55_000, datetime: new Date("2024-04-16") },
        { distance: 80_000, datetime: new Date("2024-04-01") },
        { distance: 80_000, datetime: new Date("2024-04-16") },
      ],
    }, {
      incentive: [75, 165, 105, 195, 200, 290, 0, 0],
    }));

  it("should work with global limits", async () =>
    await process({
      policy: { handler: Handler.id, max_amount: 2_200_000_00n },
      carpool: [{ distance: 5_000, driver_identity_key: "one" }],
      meta: [
        {
          key: "max_amount_restriction.global.campaign.global",
          value: 2_199_999_25n,
        },
      ],
    }, {
      incentive: [75],
      meta: [
        {
          key: "max_amount_restriction.global.campaign.global",
          value: 2_200_000_00n,
        },
        {
          key: "max_amount_restriction.0-one.month.3-2024",
          value: 75n,
        },
        {
          key: "max_amount_restriction.0-one.year.2024",
          value: 75n,
        },
      ],
    }));

  it("should work with day limits", async () =>
    await process({
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
    }, {
      incentive: [75, 75, 75, 75, 75, 75, 0],
      meta: [
        {
          key: "max_amount_restriction.global.campaign.global",
          value: 450n,
        },
        {
          key: "max_amount_restriction.0-one.month.3-2024",
          value: 450n,
        },
        {
          key: "max_amount_restriction.0-one.year.2024",
          value: 450n,
        },
      ],
    }));

  it("should work with driver month limits of 84 €", async () =>
    await process({
      policy: { handler: Handler.id },
      carpool: [
        { distance: 6_000, driver_identity_key: "one" },
        { distance: 6_000, driver_identity_key: "one" },
      ],
      meta: [
        {
          key: "max_amount_restriction.0-one.month.3-2024",
          value: 83_25n,
        },
        {
          key: "max_amount_restriction.global.campaign.global",
          value: 83_25n,
        },
      ],
    }, {
      incentive: [75, 0],
      meta: [
        {
          key: "max_amount_restriction.global.campaign.global",
          value: 84_00n,
        },
        {
          key: "max_amount_restriction.0-one.month.3-2024",
          value: 84_00n,
        },
        {
          key: "max_amount_restriction.0-one.year.2024",
          value: 150n,
        },
      ],
    }));

  it("should work with driver year limits of 1008 €", async () =>
    await process({
      policy: { handler: Handler.id },
      carpool: [
        { distance: 50_000, driver_identity_key: "one" },
        { distance: 50_000, driver_identity_key: "one" },
        { distance: 50_000, driver_identity_key: "one" },
      ],
      meta: [
        {
          key: "max_amount_restriction.0-one.month.3-2024",
          value: 83_25n,
        },
        {
          key: "max_amount_restriction.global.campaign.global",
          value: 83_25n,
        },
        {
          key: "max_amount_restriction.0-one.year.2024",
          value: 1007_99n,
        },
      ],
    }, {
      incentive: [1, 0, 0],
      meta: [
        {
          key: "max_amount_restriction.global.campaign.global",
          value: 83_26n,
        },
        {
          key: "max_amount_restriction.0-one.month.3-2024",
          value: 83_26n,
        },
        {
          key: "max_amount_restriction.0-one.year.2024",
          value: 1008_00n,
        },
      ],
    }));
});
