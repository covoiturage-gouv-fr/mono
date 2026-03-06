import { describe, it } from "dep:testing-bdd";
import { v4 as uuidV4 } from "@/lib/uuid/index.ts";
import { OperatorsEnum } from "../../interfaces/index.ts";
import { makeProcessHelper } from "../tests/macro.ts";
import { ATMBx2CCAM2026 as Handler } from "./20260101_ATMBx2CCAM_2026.ts";

// Informations générales
//
// Début : 01/01/2026
// Fin :  31/12/2026
// Budget enveloppe d’incitation : 42k€ HT
// Zone d’intervention : origine ET destination sur le territoire des communautés de communes suivantes :
//  - Communauté de communes Cluses Arve et Montagnes
//  - Communauté de communes Pays du Mont Blanc
//  - Communauté de communes Vallée de Chamonix
//  - Communauté de communes des Montagnes du Giffre
//
// Éligibilité
//
// Opérateur : BlaBlaCar Daily
// Classe de preuve : B,C
// Distance : 5 km au minimum

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
  operator_uuid: OperatorsEnum.BLABLACAR_DAILY,
  operator_class: "C",
  operator_journey_id: uuidV4(),
  operator_id: 1,
  passenger_is_over_18: true,
  passenger_has_travel_pass: true,
  driver_has_travel_pass: true,
  datetime: new Date("2026-05-15"),
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

describe("ATMBx2CCAM2026 - Exclusions", () => {
  it("unsupported operator", async () =>
    await process({
      policy: { handler: Handler.id },
      carpool: [{ operator_uuid: "not in list" }],
    }, { incentive: [0] }));

  it("distance below minimum", async () =>
    await process({
      policy: { handler: Handler.id },
      carpool: [{ distance: 1_000 }],
    }, { incentive: [0] }));

  it("trip outside AOM", async () =>
    await process({
      policy: { handler: Handler.id },
      carpool: [
        {
          start: { ...defaultPosition, aom: "not_ok" },
          end: { ...defaultPosition, aom: "not_ok" },
        },
      ],
    }, { incentive: [0] }));

  it("operator class not eligible", async () =>
    await process({
      policy: { handler: Handler.id },
      carpool: [{ operator_class: "A" }],
    }, { incentive: [0] }));

  it("trip outside of campaign dates", async () =>
    await process({
      policy: { handler: Handler.id },
      carpool: [{ datetime: new Date("2022-09-02") }],
    }, { incentive: [0] }));
});

describe("ATMBx2CCAM 2026 - Incentives", () => {
  it("early bird passenger", async () =>
    await process({
      policy: { handler: Handler.id },
      carpool: [
        { distance: 2_000 },
        { distance: 5_000 },
        { distance: 10_000 },
        { distance: 40_000 },
        { distance: 50_000 },
      ],
    }, { incentive: [0, 50, 100, 400, 400] }));

  it("old roady passenger", async () =>
    await process({
      policy: { handler: Handler.id },
      carpool: [
        { passenger_identity_key: "p0", distance: 2_000 },
        { passenger_identity_key: "p0", distance: 5_000 },
        { passenger_identity_key: "p0", distance: 10_000 },
        { passenger_identity_key: "p0", distance: 40_000 },
        { passenger_identity_key: "p0", distance: 50_000 },
      ],
      meta: [{
        key: "person_trip_counter.1-p0.campaign.global",
        value: 50n,
      }],
    }, { incentive: [0, 25, 50, 200, 200] }));

  it("threshold jumper passenger", async () =>
    await process({
      policy: { handler: Handler.id },
      carpool: [
        { passenger_identity_key: "p0", distance: 10_000 },
        { passenger_identity_key: "p0", distance: 10_000 },
        { passenger_identity_key: "p0", distance: 10_000 },
        { passenger_identity_key: "p0", distance: 10_000 },
      ],
      meta: [{
        key: "person_trip_counter.1-p0.campaign.global",
        value: 48n,
      }],
    }, { incentive: [100, 100, 50, 50] }));
});

describe("ATMBx2CCAM 2026 - Campaign global limit", () => {
  it("should limit campaign total amount to 42 000 €", async () => {
    await process({
      policy: { handler: Handler.id, max_amount: 42_000_00n },
      carpool: [{}, {}, {}],
      meta: [{
        key: "max_amount_restriction.global.campaign.global",
        value: 41_999_99n,
      }],
    }, {
      incentive: [1, 0, 0], // only 0.01 € left to reach the limit
    });
  });
});
